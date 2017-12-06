use std::fmt;
use std::mem;
use std::cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering;
use epoch::{Atomic, Owned, Shared, Guard, unprotected};

use helpers;
use tagged_usize::{TaggedUsize, AtomicTaggedUsize};

/// FIXME
const SEGMENT_SIZE: usize = 64;

/// FIXME
#[derive(Debug)]
struct PushReq<T: Send> {
    val: Atomic<T>,
    state: AtomicTaggedUsize,
}

unsafe impl<T: Send> Sync for PushReq<T> {}

/// FIXME
#[derive(Debug, Default)]
struct PopReq {
    id: AtomicUsize,
    state: AtomicTaggedUsize,
}

unsafe impl Sync for PopReq {}

/// FIXME
#[derive(Debug)]
struct Local<T: Send> {
    push: PushReq<T>,
    pop: PopReq,
}

/// FIXME
#[derive(Debug)]
struct Cell<T: Send> {
    val: Atomic<T>,
    push: Atomic<PushReq<T>>,
    pop: Atomic<PopReq>,
}

impl<T: Send> Default for Cell<T> {
    fn default() -> Self {
        Self {
            val: Atomic::null(),
            push: Atomic::null(),
            pop: Atomic::null(),
        }
    }
}

/// FIXME
struct Segment<T: Send> {
    id: usize,
    cells: [Cell<T>; SEGMENT_SIZE],
    next: Atomic<Segment<T>>,
}

impl<T: Send> Segment<T> {
    /// FIXME
    fn new(id: usize) -> Self {
        // FIXME: mem::uninitialized() is okay.
        let mut result = Self {
            id: id,
            cells: unsafe { mem::uninitialized() },
            next: Atomic::null(),
        };
        for i in 0..SEGMENT_SIZE {
            result.cells[i] = Cell::default();
        }
        result
    }
}

impl<T: Send> fmt::Debug for Segment<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // FIXME: using library?
        write!(f, "Segment {{ id: {} }}", self.id)
    }
}

/// FIXME
#[derive(Debug)]
struct Global<T: Send> {
    segments: Atomic<Segment<T>>,
    head: AtomicUsize,
    tail: AtomicUsize,
    registry: helpers::Registry<Local<T>>,
}

/// FIXME
#[derive(Debug)]
pub struct Queue<T: Send> {
    global: Arc<Global<T>>,
}

/// FIXME
#[derive(Debug)]
pub struct Handle<T: Send> {
    global: Arc<Global<T>>,
    participant: helpers::Participant<Local<T>>,
    head: Atomic<Segment<T>>,
    tail: Atomic<Segment<T>>,
    head_id: cell::Cell<usize>,
    tail_id: cell::Cell<usize>,
}

impl<T: Send> Default for PushReq<T> {
    fn default() -> Self {
        Self {
            val: Atomic::null(),
            state: AtomicTaggedUsize::default(),
        }
    }
}

impl<T: Send> Default for Local<T> {
    fn default() -> Self {
        Self {
            push: PushReq::default(),
            pop: PopReq::default(),
        }
    }
}

impl<T: Send> Global<T> {
    fn new() -> Self {
        Self {
            segments: Atomic::new(Segment::new(0)),
            head: ATOMIC_USIZE_INIT,
            tail: ATOMIC_USIZE_INIT,
            registry: helpers::Registry::new(),
        }
    }
}

impl<T: Send> Queue<T> {
    /// FIXME
    pub fn new() -> Self {
        Self { global: Arc::new(Global::new()) }
    }

    /// FIXME
    pub fn handle(&self) -> Option<Handle<T>> {
        Handle::create(self.global.clone())
    }
}

impl<T: Send> Handle<T> {
    /// FIXME
    const PATIENCE: usize = 10;
    /// FIXME
    const TAG_EMPTY: usize = 1;
    /// FIXME
    const TAG_INVALID: usize = 2;

    /// FIXME
    #[inline]
    fn create(global: Arc<Global<T>>) -> Option<Self> {
        global.registry.participant(Local::default(), unprotected()).map(|participant| {
            Self {
                global: global,
                participant,
                head: Atomic::null(),
                tail: Atomic::null(),
                head_id: cell::Cell::new(0),
                tail_id: cell::Cell::new(0),
            }
        })
    }

    /// FIXME
    pub fn clone(&self) -> Option<Self> {
        Self::create(self.global.clone())
    }

    /// FIXME
    #[inline]
    fn find_cell<'g>(
        &'g self,
        segments: &'g Atomic<Segment<T>>,
        cell_id: usize,
        guard: &'g Guard,
    ) -> &'g Cell<T> {
        let segment_id = cell_id / SEGMENT_SIZE;

        let mut s = segments.load(Ordering::Relaxed, guard); // FIXME
        let mut s_ref = unsafe { s.as_ref().unwrap() };
        for id in s_ref.id..segment_id {
            debug_assert_eq!(id, s_ref.id, "segments should be linearly aligned.");

            let next = s_ref.next.load(Ordering::Relaxed, guard);
            match unsafe { next.as_ref() } {
                Some(next_ref) => {
                    s = next;
                    s_ref = next_ref;
                }
                None => {
                    let new = Owned::new(Segment::new(id + 1));
                    s = s_ref.next
                        .compare_and_set(Shared::null(), new, Ordering::Relaxed, guard) // FIXME
                        .unwrap_or_else(|e| e.current);
                    s_ref = unsafe { s.as_ref().unwrap() };
                }
            }
        }

        segments.store(s, Ordering::Relaxed); // FIXME
        &s_ref.cells[cell_id % SEGMENT_SIZE]
    }

    /// FIXME
    #[inline]
    fn advance_end_for_linearizability(end: &AtomicUsize, cell_id: usize) {
        let mut e = end.load(Ordering::Relaxed);
        while e < cell_id {
            match end.compare_exchange(e, cell_id, Ordering::Relaxed, Ordering::Relaxed) { // FIXME
                Ok(_) => break,
                Err(current) => e = current,
            }
        }
    }

    // FIXME(jeehoonkang): REVISED SO FAR

    /// FIXME
    pub fn push(&self, val: T, guard: &Guard) {
        self.normalize(guard);

        let mut val = Owned::new(val);
        let mut cell_id = 0;
        for _ in 0..Self::PATIENCE {
            match self.push_fast(val, guard) {
                Ok(_) => return,
                Err((v, id)) => {
                    val = v;
                    cell_id = id;
                }
            }
        }

        self.push_slow(val, cell_id, guard);
    }

    #[inline]
    fn push_fast(&self, val: Owned<T>, guard: &Guard) -> Result<(), (Owned<T>, usize)> {
        let i = self.global.tail.fetch_add(1, Ordering::Relaxed);
        let cell = self.find_cell(&self.tail, i, guard);
        cell.val
            .compare_and_set(Shared::null(), val, Ordering::Relaxed, guard)
            .map(|_| ())
            .map_err(|e| (e.new, i))
    }

    #[cold]
    #[inline]
    fn push_slow(&self, val: Owned<T>, mut cell_id: usize, guard: &Guard) {
        let push = &self.participant.get().push;
        let val = val.into_shared(guard);
        push.val.store(val, Ordering::Relaxed);
        push.state.store(TaggedUsize::new(cell_id, true), Ordering::Relaxed);

        let tail = self.tail.load(Ordering::Relaxed, guard);
        let original_tail = Atomic::null();
        original_tail.store(tail, Ordering::Relaxed);

        loop {
            let i = self.global.tail.fetch_add(1, Ordering::Relaxed);
            let cell = self.find_cell(&original_tail, i, guard);
            if cell.push
                .compare_and_set(
                    Shared::null(),
                    Shared::from(push as *const _),
                    Ordering::Relaxed,
                    guard,
                )
                .is_ok() && cell.val.load(Ordering::Relaxed, guard).is_null()
            {
                let _ = self.try_to_claim_req(&push.state, cell_id, i);
                break;
            }

            let (id, pending) = push.state.load(Ordering::Relaxed);
            cell_id = id;
            if !pending {
                break;
            }
        }

        let cell = self.find_cell(&self.tail, cell_id, guard);
        self.push_commit(cell, val, cell_id);
    }

    #[inline]
    fn normalize(&self, guard: &Guard) {
        let segments = self.global.segments.load(Ordering::Relaxed, guard);
        let segments_ref = unsafe { segments.as_ref().unwrap() };

        let tail = self.tail.load(Ordering::Relaxed, guard);
        let tail_ref = unsafe { tail.as_ref() };

        match tail_ref {
            None => {
                self.tail.store(segments, Ordering::Relaxed);
            }
            Some(tail_ref) => {
                if tail_ref.id < segments_ref.id {
                    self.tail.store(segments, Ordering::Relaxed);
                }
            }
        }

        let head = self.head.load(Ordering::Relaxed, guard);
        let head_ref = unsafe { head.as_ref() };

        match head_ref {
            None => {
                self.head.store(segments, Ordering::Relaxed);
            }
            Some(head_ref) => {
                if head_ref.id < segments_ref.id {
                    self.head.store(segments, Ordering::Relaxed);
                }
            }
        }
    }

    #[inline]
    fn try_to_claim_req<'g>(
        &'g self,
        state: &'g AtomicTaggedUsize,
        cell_id: usize,
        i: usize,
    ) -> Result<(), (usize, bool)> {
        // This should be a strong update.
        state.compare_and_set(
            TaggedUsize::new(cell_id, true),
            TaggedUsize::new(i, false),
            Ordering::Relaxed,
        )
    }

    #[inline]
    fn push_commit<'g>(&'g self, c: &'g Cell<T>, val: Shared<'g, T>, cell_id: usize) {
        Self::advance_end_for_linearizability(&self.global.tail, cell_id + 1);
        c.val.store(val, Ordering::Relaxed);
    }

    pub fn try_pop(&self, guard: &Guard) -> Option<T> {
        self.normalize(guard);

        let mut cell_id = 0;
        let result = self.try_pop_fast(&mut cell_id, guard)
            .or_else(|| self.try_pop_slow(cell_id, guard));
        if result.is_some() {
            self.help_pop(guard);
        }
        result.map(|result| result.into_box().into())
    }

    #[inline]
    fn try_pop_fast<'g>(&'g self, id: &'g mut usize, guard: &'g Guard) -> Option<Owned<T>> {
        for _ in 0..Self::PATIENCE {
            let i = self.global.head.fetch_add(1, Ordering::Relaxed);
            let cell = self.find_cell(&self.head, i, guard);
            let result = self.help_push(cell, i, guard);

            match result.tag() {
                Self::TAG_EMPTY => return None,
                Self::TAG_INVALID => *id = i,
                _ => {
                    if cell.pop
                        .compare_and_set(
                            Shared::null(),
                            Shared::null().with_tag(Self::TAG_INVALID),
                            Ordering::Relaxed,
                            guard,
                        )
                        .is_ok()
                    {
                        return Some(unsafe { result.into() });
                    } else {
                        *id = i;
                    }
                }
            }
        }
        None
    }

    #[inline]
    fn try_pop_slow(&self, cell_id: usize, guard: &Guard) -> Option<Owned<T>> {
        let req = &self.participant.get().pop;
        req.id.store(cell_id, Ordering::Relaxed);
        req.state.store(TaggedUsize::new(cell_id, true), Ordering::Relaxed);

        self.help_pop(guard);

        let i = req.state.load(Ordering::Relaxed).0;
        let cell = self.find_cell(&self.head, i, guard);
        Self::advance_end_for_linearizability(&self.global.head, cell_id + 1);

        let result = cell.val.load(Ordering::Relaxed, guard);
        unsafe { result.as_ref().map(|_| result.into_owned()) }
    }

    fn help_push<'g>(&'g self, cell: &'g Cell<T>, i: usize, guard: &'g Guard) -> Shared<'g, T> {
        let cell_val = match cell.val.compare_and_set(
            Shared::null(),
            Shared::null().with_tag(Self::TAG_INVALID),
            Ordering::Relaxed,
            guard,
        ) {
            Ok(_) => Shared::null().with_tag(Self::TAG_INVALID),
            Err(e) => {
                let val = e.current;
                if val != Shared::null().with_tag(Self::TAG_INVALID) {
                    return val;
                }
                val
            }
        };

        let push = &self.participant.get().push;
        let (mut id, pending) = push.state.load(Ordering::Relaxed);
        let mut cell_req = cell.push.load(Ordering::Relaxed, guard);

        if cell_req == Shared::null() {
            let (peer, peer_id, peer_pending) = loop {
                let peer = self.participant.peer();
                let (peer_id, peer_pending) = peer.push.state.load(Ordering::Relaxed);

                if id == 0 || id == peer_id { break (peer, peer_id, peer_pending); }

                push.state.store(TaggedUsize::new(0, pending), Ordering::Relaxed);
                id = 0;
                self.participant.next();
            };

            if peer_pending &&
                peer_id <= i &&
                cell.push.compare_and_set(
                    Shared::null(),
                    Shared::from(&peer.push as *const _),
                    Ordering::Relaxed,
                    guard,
                )
                .is_err() {
                push.state.store(TaggedUsize::new(peer_id, pending), Ordering::Relaxed);
            } else {
                self.participant.next();
            }

            cell_req = cell.push.load(Ordering::Relaxed, guard);
            if cell_req == Shared::null() {
                let _ = cell.push.compare_and_set(
                    Shared::null(),
                    Shared::null().with_tag(Self::TAG_INVALID),
                    Ordering::Relaxed,
                    guard,
                );
            }
        }

        cell_req = cell.push.load(Ordering::Relaxed, guard);
        if cell_req.tag() == Self::TAG_INVALID {
            let tail = self.global.tail.load(Ordering::Relaxed);
            return Shared::null().with_tag(if tail <= i { Self::TAG_EMPTY } else { Self::TAG_INVALID });
        }

        let (id, pending) = push.state.load(Ordering::Relaxed);
        let v = push.val.load(Ordering::Relaxed, guard);

        if peer_id > i {
            if cell_val.tag() == Self::TAG_INVALID && self.global.tail.load(Ordering::Relaxed) <= i {
                return Shared::null().with_tag(Self::TAG_EMPTY);
            }
        } else {
            if self.try_to_claim_req(&push.state, peer_id, i).is_ok() ||
                (peer_id == i && !peer_pending && cell_val.tag() == Self::TAG_INVALID) {
                self.push_commit(cell, v, i);
            }
        }
        cell_val
    }

    #[inline]
    fn help_pop(&self, guard: &Guard) {
        unimplemented!()
    }
}


#[cfg(test)]
mod tests {
    use crossbeam_utils::guard;
    use epoch::Collector;
    use super::Queue;

    const THREADS: usize = 8;
    const COUNT: usize = 1000;

    #[test]
    fn test_push() {
        let collector = Collector::new();
        let queue = Queue::new();

        let threads = (0..THREADS)
            .map(|_| {
                scoped::scope(|guard| {
                    guard.spawn(|| {
                        let handle = collector.handle();
                        let queue = queue.handle();
                        for _ in 0..COUNT {
                            handle.pin(|guard| { queue.push(42, guard); });
                        }
                    })
                })
            })
            .collect::<Vec<_>>();

        for t in threads {
            t.join();
        }
    }
}
