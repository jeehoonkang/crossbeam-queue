use std::fmt;
use std::mem;
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
    head: Atomic<Segment<T>>,
    tail: Atomic<Segment<T>>,
    // FIXME(jeehoonkang): is ids can wrap around, thus accessing freed memory..
    // FIXME(jeehoonkang): too fragile interface..
    head_id: AtomicUsize,
    tail_id: AtomicUsize,
}

unsafe impl<T: Send> Sync for Local<T> {}

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
    // FIXME(jeehoonkang): a single peer for both push/try_pop
    local: helpers::Participant<Local<T>>,
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
            head: Atomic::null(),
            tail: Atomic::null(),
            head_id: AtomicUsize::new(-1isize as usize),
            tail_id: AtomicUsize::new(-1isize as usize),
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
        unsafe {
            global.registry.participant(Local::default(), unprotected()).map(|local| {
                Self {
                    global: global,
                    local,
                }
            })
        }
    }

    /// FIXME
    pub fn clone(&self) -> Option<Self> {
        Self::create(self.global.clone())
    }

    /// FIXME
    #[inline]
    fn find_cell<'g>(
        segment: &'g Atomic<Segment<T>>,
        cell_id: usize,
        guard: &'g Guard,
    ) -> &'g Cell<T> {
        let segment_id = cell_id / SEGMENT_SIZE;

        let mut s = segment.load(Ordering::Relaxed, guard); // FIXME
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

        segment.store(s, Ordering::Relaxed); // FIXME
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

    /// FIXME: update head/tail if they are stale (more so than global.segments).
    #[inline]
    fn normalize(&self, guard: &Guard) {
        let local = self.local.get();
        let segments = self.global.segments.load(Ordering::Relaxed, guard); // FIXME
        let segments_ref = unsafe { segments.as_ref().unwrap() };

        let head_id = local.head_id.load(Ordering::Relaxed);
        if (head_id.wrapping_sub(segments_ref.id) as isize) < 0 {
            local.head.store(segments, Ordering::Relaxed); // FIXME
            local.head_id.store(segments_ref.id, Ordering::Relaxed);
        }

        let tail_id = local.tail_id.load(Ordering::Relaxed);
        if (tail_id.wrapping_sub(segments_ref.id) as isize) < 0 {
            local.tail.store(segments, Ordering::Relaxed); // FIXME
            local.tail_id.store(segments_ref.id, Ordering::Relaxed);
        }
    }

    /// FIXME
    pub fn push(&self, val: T, guard: &Guard) {
        self.normalize(guard);
        self.push_fast(Owned::new(val), guard)
            .unwrap_or_else(|(val, cell_id)| self.push_slow(val, cell_id, guard))
    }

    /// FIXME
    #[inline]
    fn push_fast(&self, mut val: Owned<T>, guard: &Guard) -> Result<(), (Owned<T>, usize)> {
        let local = self.local.get();
        let mut cell_id = 0;
        for _ in 0..Self::PATIENCE {
            let i = self.global.tail.fetch_add(1, Ordering::Relaxed); // FIXME
            let cell = Self::find_cell(&local.tail, i, guard);
            local.tail_id.store(i / SEGMENT_SIZE, Ordering::Relaxed);
            match cell.val.compare_and_set(Shared::null(), val, Ordering::Relaxed, guard) { // FIXME
                Ok(_) => return Ok(()),
                Err(e) => {
                    cell_id = i;
                    val = e.new;
                }
            }
        }
        Err((val, cell_id))
    }

    /// FIXME
    #[cold]
    #[inline]
    fn push_slow(&self, val: Owned<T>, mut cell_id: usize, guard: &Guard) {
        let local = self.local.get();
        let push = &local.push;
        let val = val.into_shared(guard);
        push.val.store(val, Ordering::Relaxed); // FIXME
        push.state.store(TaggedUsize::new(cell_id, true), Ordering::Relaxed); // FIXME

        let original_tail = local.tail.clone();
        loop {
            let i = self.global.tail.fetch_add(1, Ordering::Relaxed); // FIXME
            let cell = Self::find_cell(&original_tail, i, guard);
            if cell.push
                .compare_and_set(
                    Shared::null(),
                    Shared::from(push as *const _),
                    Ordering::Relaxed,
                    guard,
                ).is_ok() &&
                cell.val.load(Ordering::Relaxed, guard).is_null() // FIXME
            {
                let _ = Self::try_to_claim_req(&push.state, cell_id, i);
                break;
            }

            let (id, pending) = push.state.load(Ordering::Relaxed).decompose(); // FIXME
            cell_id = id;
            if !pending { break; }
        }

        let cell = Self::find_cell(&local.tail, cell_id, guard);
        local.tail_id.store(cell_id / SEGMENT_SIZE, Ordering::Relaxed);
        self.push_commit(cell, val, cell_id);
    }

    /// FIXME
    #[inline]
    fn try_to_claim_req<'g>(
        state: &'g AtomicTaggedUsize,
        cell_id: usize,
        i: usize,
    ) -> Result<(), (usize, bool)> {
        // This should be a strong update.
        state.compare_and_set(
            TaggedUsize::new(cell_id, true),
            TaggedUsize::new(i, false),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) // FIXME
            .map(|_| ())
            .map_err(|current| current.decompose())
    }

    /// FIXME
    #[inline]
    fn push_commit<'g>(&'g self, c: &'g Cell<T>, val: Shared<'g, T>, cell_id: usize) {
        Self::advance_end_for_linearizability(&self.global.tail, cell_id + 1);
        c.val.store(val, Ordering::Relaxed); // FIXME
    }

    /// FIXME
    pub fn try_pop(&self, guard: &Guard) -> Option<T> {
        self.normalize(guard);
        self.try_pop_fast(guard)
            .or_else(|cell_id| self.try_pop_slow(cell_id, guard).ok_or(()))
            .ok()
            .map(|result| {
                self.help_pop(self.local.peer(), guard);
                self.local.next(guard);
                *result.into_box()
            })
    }

    /// FIXME
    #[inline]
    fn try_pop_fast<'g>(&'g self, guard: &'g Guard) -> Result<Owned<T>, usize> {
        let local = self.local.get();
        let mut id = 0;
        for _ in 0..Self::PATIENCE {
            let i = self.global.head.fetch_add(1, Ordering::Relaxed); // FIXME
            let cell = Self::find_cell(&local.head, i, guard);
            local.head_id.store(i / SEGMENT_SIZE, Ordering::Relaxed);
            let result = self.help_push(self.local.get(), cell, i, guard);

            match result.tag() {
                Self::TAG_EMPTY => return Err(id),
                Self::TAG_INVALID => id = i,
                _ => {
                    if cell.pop
                        .compare_and_set(
                            Shared::null(),
                            Shared::null().with_tag(Self::TAG_INVALID),
                            Ordering::Relaxed,
                            guard,
                        ) // FIXME
                        .is_ok()
                    {
                        return Ok(unsafe { result.into_owned() });
                    }
                    id = i;
                }
            }
        }
        Err(id)
    }

    /// FIXME
    #[inline]
    fn try_pop_slow(&self, cell_id: usize, guard: &Guard) -> Option<Owned<T>> {
        let local = self.local.get();
        let req = &local.pop;
        req.id.store(cell_id, Ordering::Relaxed);
        req.state.store(TaggedUsize::new(cell_id, true), Ordering::Relaxed);

        self.help_pop(self.local.get(), guard); // FIXME: designating helpee?

        let (i, _) = req.state.load(Ordering::Relaxed).decompose();
        let cell = Self::find_cell(&local.head, i, guard);
        local.head_id.store(i / SEGMENT_SIZE, Ordering::Relaxed);
        Self::advance_end_for_linearizability(&self.global.head, cell_id + 1);

        let result = cell.val.load(Ordering::Relaxed, guard);
        unsafe { result.as_ref().map(|_| result.into_owned()) }
        // FIXME: do we need ManuallyDrop?
    }

    /// FIXME
    fn help_push<'g>(&'g self, helpee: &'g Local<T>, cell: &'g Cell<T>, i: usize, guard: &'g Guard) -> Shared<'g, T> {
        match cell.val.compare_and_set(
            Shared::null(),
            Shared::null().with_tag(Self::TAG_INVALID),
            Ordering::Relaxed, // FIXME
            guard,
        ) {
            Ok(_) => {},
            Err(e) => {
                let val = e.current;
                if val != Shared::null().with_tag(Self::TAG_INVALID) { return val; }
            }
        };

        // cell.val is INVALID, so help slow-path pushes
        let push = &helpee.push;
        let (mut id, pending) = push.state.load(Ordering::Relaxed).decompose(); // FIXME

        let mut cell_push = cell.push.load(Ordering::Relaxed, guard); // FIXME
        if cell_push == Shared::null() {
            let (peer, peer_id, peer_pending) = loop {
                let peer = self.local.peer();
                let (peer_id, peer_pending) = peer.push.state.load(Ordering::Relaxed).decompose(); // FIXME

                if id == 0 || id == peer_id { break (peer, peer_id, peer_pending); }
                // (peer, peer_id, peer_pending); } FIXME

                push.state.store(TaggedUsize::new(0, pending), Ordering::Relaxed);
                id = 0;
                self.local.next(guard);
            };

            if peer_pending && peer_id <= i &&
                cell.push.compare_and_set(
                    Shared::null(),
                    Shared::from(&peer.push as *const _),
                    Ordering::Relaxed, // FIXME
                    guard,
                ).is_err() {
                push.state.store(TaggedUsize::new(peer_id, pending), Ordering::Relaxed); // FIXME
            } else {
                self.local.next(guard);
            }

            // FIXME: read cell.push first?
            let _ = cell.push.compare_and_set(
                Shared::null(),
                Shared::null().with_tag(Self::TAG_INVALID),
                Ordering::Relaxed, // FIXME
                guard,
            );
        }

        cell_push = cell.push.load(Ordering::Relaxed, guard); // FIXME
        if cell_push.tag() == Self::TAG_INVALID {
            let tail = self.global.tail.load(Ordering::Relaxed); // FIXME
            return Shared::null().with_tag(if tail <= i { Self::TAG_EMPTY } else { Self::TAG_INVALID });
        }

        let cell_push_ref = unsafe { cell_push.as_ref().unwrap() };
        let (cell_id, cell_pending) = cell_push_ref.state.load(Ordering::Relaxed).decompose(); // FIXME
        let cell_val = cell.val.load(Ordering::Relaxed, guard); // FIXME

        if cell_id > i {
            if cell_val.tag() == Self::TAG_INVALID && self.global.tail.load(Ordering::Relaxed) <= i { // FIXME
                return Shared::null().with_tag(Self::TAG_EMPTY);
            }
        } else {
            if Self::try_to_claim_req(&cell_push_ref.state, cell_id, i).is_ok() || (cell_id == i && !cell_pending && cell_val.tag() == Self::TAG_INVALID) {
                let val = cell_push_ref.val.load(Ordering::Relaxed, guard); // FIXME
                self.push_commit(cell, val, i);
            }
        }

        cell_val
    }

    // FIXME(jeehoonkang): REVISED SO FAR

    /// FIXME
    #[inline]
    fn help_pop(&self, helpee: &Local<T>, guard: &Guard) {
        let pop = &helpee.pop;
        let id = pop.id.load(Ordering::Relaxed);
        let (idx, pending) = pop.state.load(Ordering::Relaxed).decompose(); // FIXME

        if !pending || idx < id { return; }

        let mut state = pop.state.load(Ordering::Relaxed).decompose(); // FIXME
        let mut prior = id;
        let mut i = id;
        let mut cand = 0;
        while cand == 0 && state.0 == prior {
            i = i + 1;
            let cell = Self::find_cell(&helpee.head, i, guard);
            let val = self.help_push(helpee, cell, i, guard);
            if val.tag() == Self::TAG_EMPTY || (val.tag() != Self::TAG_INVALID && cell.pop.load(Ordering::Relaxed, guard).tag() == Self::TAG_EMPTY) {
                cand = i;
            } else {
                state = pop.state.load(Ordering::Relaxed).decompose(); // FIXME
            }

            if cand != 0 {
                state = pop.state.compare_and_set(
                    TaggedUsize::new(prior, true),
                    TaggedUsize::new(cand, true),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                    .unwrap_or_else(|current| current)
                    .decompose();
            }

            if !state.1 || pop.id.load(Ordering::Relaxed) != id { return; }

            let cell = Self::find_cell(&helpee.head, state.0, guard);
            let pop_shared = Shared::from(pop as *const _);
            if cell.val.load(Ordering::Relaxed, guard).tag() == Self::TAG_INVALID { return; }
            if cell.pop.compare_and_set(
                Shared::null(),
                pop_shared,
                Ordering::Relaxed,
                guard,
            ).is_ok() {
                return;
            }
            if cell.pop.load(Ordering::Relaxed, guard) == pop_shared { return; }

            prior = state.0;
            if state.0 >= i {
                cand = 0;
                i = state.0;
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use crossbeam_utils::scoped;
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
                scoped::scope(|scope| {
                    scope.spawn(|| {
                        let collector_handle = collector.handle();
                        let queue_handle = queue.handle().unwrap();
                        for _ in 0..COUNT {
                            let guard = collector_handle.pin();
                            queue_handle.push(42, &guard);
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
