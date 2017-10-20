//! Michael's lock-free linked list.
//!
//! Michael.  High Performance Dynamic Lock-Free Hash Tables and List-Based Sets.  SPAA 2002.
//! http://dl.acm.org/citation.cfm?id=564870.564881

use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use epoch::{Atomic, Owned, Shared, Guard, unprotected};
use crossbeam_utils::cache_padded::CachePadded;


/// An entry in the linked list.
struct NodeInner<T> {
    /// The data in the entry.
    data: T,

    /// The next entry in the linked list.
    /// If the tag is 1, this entry is marked as deleted.
    next: Atomic<Node<T>>,
}

unsafe impl<T> Send for NodeInner<T> {}

#[derive(Debug)]
pub struct Node<T>(CachePadded<NodeInner<T>>);

#[derive(Debug)]
pub struct List<T> {
    head: Atomic<Node<T>>,
}

pub struct Iter<'g, T: 'g> {
    /// The guard in which the iterator is operating.
    guard: &'g Guard,

    /// Pointer from the predecessor to the current entry.
    pred: &'g Atomic<Node<T>>,

    /// The current entry.
    curr: Shared<'g, Node<T>>,
}

#[derive(PartialEq, Debug)]
pub enum IterError {
    /// Iterator lost a race in deleting a node by a concurrent iterator.
    LostRace,
}

impl<T> Node<T> {
    /// Returns the data in this entry.
    fn new(data: T) -> Self {
        Node(CachePadded::new(NodeInner {
            data,
            next: Atomic::null(),
        }))
    }

    pub fn get_data(&self) -> &T {
        &self.0.data
    }

    pub fn get_next(&self) -> &Atomic<Node<T>> {
        &self.0.next
    }

    /// Marks this entry as deleted.
    pub fn delete(&self, guard: &Guard) {
        self.0.next.fetch_or(1, Release, guard);
    }
}

impl<T> List<T> {
    /// Returns a new, empty linked list.
    pub fn new() -> Self {
        List { head: Atomic::null() }
    }

    #[inline]
    pub fn get_head<'g>(&'g self, guard: &'g Guard) -> Shared<'g, Node<T>> {
        self.head.load(Relaxed, guard)
    }

    /// Inserts `data` into the list.
    #[inline]
    fn insert_internal<'g>(
        to: &'g Atomic<Node<T>>,
        data: T,
        guard: &'g Guard,
    ) -> Shared<'g, Node<T>> {
        let mut cur = Owned::new(Node::new(data));
        let mut next = to.load(Relaxed, guard);

        loop {
            cur.0.next.store(next, Relaxed);
            match to.compare_and_set_weak(next, cur, Release, guard) {
                Ok(cur) => return cur,
                Err((n, c)) => {
                    next = n;
                    cur = c;
                }
            }
        }
    }

    /// Inserts `data` into the head of the list.
    #[inline]
    pub fn insert<'g>(&'g self, data: T, guard: &'g Guard) -> Shared<'g, Node<T>> {
        Self::insert_internal(&self.head, data, guard)
    }

    /// Inserts `data` after `after` into the list.
    #[inline]
    #[allow(dead_code)]
    pub fn insert_after<'g>(
        &'g self,
        after: &'g Atomic<Node<T>>,
        data: T,
        guard: &'g Guard,
    ) -> Shared<'g, Node<T>> {
        Self::insert_internal(after, data, guard)
    }

    /// Returns an iterator over all data.
    ///
    /// # Caveat
    ///
    /// Every datum that is inserted at the moment this function is called and persists at least
    /// until the end of iteration will be returned. Since this iterator traverses a lock-free
    /// linked list that may be concurrently modified, some additional caveats apply:
    ///
    /// 1. If a new datum is inserted during iteration, it may or may not be returned.
    /// 2. If a datum is deleted during iteration, it may or may not be returned.
    /// 3. It may not return all data if a concurrent thread continues to iterate the same list.
    pub fn iter<'g>(&'g self, guard: &'g Guard) -> Iter<'g, T> {
        let pred = &self.head;
        let curr = pred.load(Acquire, guard);
        Iter { guard, pred, curr }
    }
}

impl<T> Drop for List<T> {
    fn drop(&mut self) {
        unsafe {
            let guard = unprotected();
            let mut curr = self.head.load(Relaxed, guard);
            while let Some(c) = curr.as_ref() {
                let succ = c.0.next.load(Relaxed, guard);
                drop(curr.into());
                curr = succ;
            }
        }
    }
}

impl<'g, T> Iterator for Iter<'g, T> {
    type Item = Result<&'g Node<T>, IterError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(c) = unsafe { self.curr.as_ref() } {
            let succ = c.0.next.load(Acquire, self.guard);

            if succ.tag() == 1 {
                // This entry was removed. Try unlinking it from the list.
                let succ = succ.with_tag(0);

                match self.pred.compare_and_set_weak(
                    self.curr,
                    succ,
                    Acquire,
                    self.guard,
                ) {
                    Ok(_) => {
                        unsafe {
                            // Deferred drop of `T` is scheduled here.
                            // This is okay because `.delete()` can be called only if `T: 'static`.
                            let p = self.curr;
                            self.guard.defer(move || p.into());
                        }
                        self.curr = succ;
                    }
                    Err((succ, _)) => {
                        // We lost the race to delete the entry by a concurrent iterator. Set
                        // `self.curr` to the updated pointer, and report the lost.
                        self.curr = succ;
                        return Some(Err(IterError::LostRace));
                    }
                }

                continue;
            }

            // Move one step forward.
            self.pred = &c.0.next;
            self.curr = succ;

            return Some(Ok(&c));
        }

        // We reached the end of the list.
        None
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

        handle.pin(|guard| {
            let p2 = l.insert(2, guard);
            let n2 = unsafe { p2.as_ref().unwrap() };
            let _p3 = l.insert_after(&n2.0.next, 3, guard);
            let _p1 = l.insert(1, guard);

            let mut iter = l.iter(guard);
            assert!(iter.next().is_some());
            assert!(iter.next().is_some());
            assert!(iter.next().is_some());
            assert!(iter.next().is_none());

            n2.delete(guard);

            let mut iter = l.iter(guard);
            assert!(iter.next().is_some());
            assert!(iter.next().is_some());
            assert!(iter.next().is_none());
        });
    }
}
