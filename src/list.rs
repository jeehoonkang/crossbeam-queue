//! Michael's lock-free linked list.
//!
//! Michael.  High Performance Dynamic Lock-Free Hash Tables and List-Based Sets.  SPAA 2002.
//! http://dl.acm.org/citation.cfm?id=564870.564881

use core::sync::atomic::Ordering;

use crossbeam_utils::cache_padded::CachePadded;
use epoch::{Atomic, Owned, Shared, Guard, Shield, unprotected};

#[derive(Debug)]
struct NodeInner<T> {
    /// The data in the entry.
    data: T,

    /// The next entry in the linked list.
    /// If the tag is 1, this entry is marked as deleted.
    next: Atomic<Node<T>>,
}

unsafe impl<T> Send for NodeInner<T> {}

/// A node in a linked list.
#[derive(Debug)]
pub struct Node<T>(CachePadded<NodeInner<T>>);

/// A lock-free linked list of type `T`.
#[derive(Debug)]
pub struct List<T> {
    /// The head of the linked list.
    head: Atomic<Node<T>>,
}

/// An error that occurs during iteration over the list.
pub struct Iter<T> {
    /// Pointer from the predecessor to the current entry.
    pred: Shield<Node<T>>,

    /// The current entry.
    curr: Shield<Node<T>>,

    /// The list head, needed for restarting iteration.
    head: *const Atomic<Node<T>>,
}

impl<T> Node<T> {
    /// Returns the data in this entry.
    fn new(data: T) -> Self {
        Node(CachePadded::new(NodeInner {
            data,
            next: Atomic::null(),
        }))
    }

    pub fn get(&self) -> &T {
        &self.0.data
    }

    /// Marks this entry as deleted.
    pub fn delete(&self, guard: &Guard) {
        self.0.next.fetch_or(1, Ordering::Release, guard);
    }
}

impl<T> List<T> {
    /// Returns a new, empty linked list.
    pub fn new() -> Self {
        List { head: Atomic::null() }
    }

    /// Inserts `data` into the head of the list.
    #[inline]
    pub fn insert<'g>(&'g self, data: T, guard: &'g Guard) -> Shared<'g, Node<T>> {
        // Insert right after head, i.e. at the beginning of the list.
        let to = &self.head;
        // Create a node containing `data`.
        let node = Owned::new(Node::new(data)).into_shared(guard);
        // Create a reference to the node.
        let node_ref = unsafe { node.as_ref().unwrap() };
        // Read the current successor of where we want to insert.
        let mut next = to.load(Ordering::Relaxed, guard);

        loop {
            // Set the Entry of the to-be-inserted element to point to the previous successor of
            // `to`.
            node_ref.0.next.store(next, Ordering::Relaxed);
            match to.compare_and_set_weak(next, node, Ordering::Release, guard) {
                Ok(node) => return node,
                // We lost the race or weak CAS failed spuriously. Update the successor and try
                // again.
                Err(err) => next = err.current,
            }
        }
    }

    /// Returns an iterator over all objects.
    ///
    /// # Caveat
    ///
    /// Every object that is inserted at the moment this function is called and persists at least
    /// until the end of iteration will be returned. Since this iterator traverses a lock-free
    /// linked list that may be concurrently modified, some additional caveats apply:
    ///
    /// 1. If a new object is inserted during iteration, it may or may not be returned.
    /// 2. If an object is deleted during iteration, it may or may not be returned.
    /// 3. The iteration may be aborted when it lost in a race condition. In this case, the winning
    ///    thread will continue to iterate over the same list.
    pub fn iter(&self, guard: &Guard) -> Option<Iter<T>> {
        let head = &self.head;
        let curr = head.load(Ordering::Acquire, guard);
        guard.shield(Shared::null()).and_then(|pred| {
            guard.shield(curr).map(|curr| {
                Iter { pred, curr, head }
            })
        })
    }
}

impl<T> Drop for List<T> {
    fn drop(&mut self) {
        unsafe {
            let mut curr = self.head.load(Ordering::Relaxed, unprotected());
            while let Some(c) = curr.as_ref() {
                let succ = c.0.next.load(Ordering::Relaxed, unprotected());
                drop(curr.into_owned());
                curr = succ;
            }
        }
    }
}

/// The result of an iteration.
#[derive(Debug)]
pub enum IterResult {
    Some,
    None,
    /// A concurrent thread modified the state of the list at the same place that this iterator was
    /// inspecting. Subsequent iteration will restart from the beginning of the list.
    Restart,
}

impl IterResult {
    pub fn is_some(&self) -> bool {
        match *self {
            IterResult::Some => true,
            _ => false,
        }
    }

    pub fn is_none(&self) -> bool {
        match *self {
            IterResult::None => true,
            _ => false,
        }
    }
}

impl<T> Iter<T> {
    #[inline]
    pub fn restart<'g>(&'g self, guard: &'g Guard)
    where
        T: 'g,
    {
        self.pred.defend(Shared::null());
        self.curr.defend(unsafe { &*self.head }.load(Ordering::Acquire, guard));
    }

    #[inline]
    pub fn get<'g>(&'g self) -> Shared<'g, Node<T>>
        where
        T: 'g,
    {
        self.curr.get()
    }

    pub fn next<'g>(&'g self, guard: &'g Guard) -> IterResult
    where
        T: 'g,
    {
        let pred = unsafe { self.pred.get().as_ref() }
            .map(|pred| &pred.0.next)
            .unwrap_or(unsafe { &*self.head });

        // If `pred` is changed, restart from `head`.
        if pred.load(Ordering::Relaxed, guard) != self.curr.get() {
            self.restart(guard);
            return IterResult::Restart;
        }

        while let Some(curr_ref) = unsafe { self.curr.get().as_ref() } {
            let succ = curr_ref.0.next.load(Ordering::Acquire, guard);

            if succ.tag() == 1 {
                // This node was removed. Try unlinking it from the list.
                let succ = succ.with_tag(0);

                // The tag should never be zero, because removing a node after a logically deleted
                // node leaves the list in an invalid state.
                debug_assert!(self.curr.get().tag() == 0);

                match pred.compare_and_set(
                    self.curr.get(),
                    succ,
                    Ordering::Acquire,
                    guard,
                ) {
                    Ok(_) => {
                        // We succeeded in unlinking this element from the list, so we have to
                        // schedule deallocation. Deferred drop is okay, because `list.delete()`
                        // can only be called if `T: 'static`.
                        unsafe {
                            let curr = self.curr.get();
                            guard.defer(move |_| curr.into_owned());
                        }

                        // Move over the removed by only advancing `curr`, not `pred`.
                        self.curr.defend(succ);
                        continue;
                    }
                    Err(_) => {
                        // A concurrent thread modified the predecessor node. Since it might've
                        // been deleted, we need to restart from `head`.
                        self.restart(guard);
                        return IterResult::Restart;
                    }
                }
            }

            // Move one step forward.
            self.pred.swap(&self.curr);
            self.curr.defend(succ);

            return IterResult::Some;
        }

        // We reached the end of the list.
        IterResult::None
    }
}

impl<T> Default for List<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use epoch::Collector;
    use super::*;

    #[test]
    fn insert_iter_delete_iter() {
        let l: List<i64> = List::new();

        let collector = Collector::new();
        let handle = collector.handle();
        let guard = handle.pin();

        let _p3 = l.insert(3, &guard);
        let p2 = l.insert(2, &guard);
        let _p1 = l.insert(1, &guard);

        let mut iter = l.iter(&guard).unwrap();
        assert!(iter.next(&guard).is_some());
        assert!(iter.next(&guard).is_some());
        assert!(iter.next(&guard).is_some());
        assert!(iter.next(&guard).is_none());

        unsafe { p2.as_ref().unwrap().delete(&guard); }

        let mut iter = l.iter(&guard).unwrap();
        assert!(iter.next(&guard).is_some());
        assert!(iter.next(&guard).is_some());
        assert!(iter.next(&guard).is_none());
    }
}
