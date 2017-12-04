use std::fmt;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering;
use epoch::{Atomic, Owned, Shared, Guard};

use helpers;

const SEGMENT_SIZE: usize = 64;

#[derive(Debug, Default)]
struct State(AtomicUsize);

impl State {
    fn encode(data: usize, bit: bool) -> usize {
        (data << 1) + (if bit { 1 } else { 0 })
    }

    fn decode(val: usize) -> (usize, bool) {
        (val >> 1, (val & 1) == 1)
    }

    #[inline]
    pub fn load(&self, ordering: Ordering) -> (usize, bool) {
        Self::decode(self.0.load(ordering))
    }

    #[inline]
    pub fn store(&self, data: usize, bit: bool, ordering: Ordering) {
        self.0.store(Self::encode(data, bit), ordering)
    }

    #[inline]
    pub fn compare_exchange(
        &self,
        data1: usize,
        bit1: bool,
        data2: usize,
        bit2: bool,
        success: Ordering,
        failure: Ordering,
    ) -> Result<(), (usize, bool)> {
        self.0
            .compare_exchange(
                Self::encode(data1, bit1),
                Self::encode(data2, bit2),
                success,
                failure,
            )
            .map(|_| ())
            .map_err(|old| Self::decode(old))
    }
}

#[derive(Debug)]
struct PushReq<T: Send> {
    val: Atomic<T>,
    state: State,
}

unsafe impl<T: Send> Sync for PushReq<T> {}

#[derive(Debug, Default)]
struct PopReq {
    id: AtomicUsize,
    state: State,
}

unsafe impl Sync for PopReq {}

#[derive(Debug)]
struct Registry<T: Send> {
    push: PushReq<T>,
    pop: PopReq,
    lower_bound: AtomicUsize,
}

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

struct Segment<T: Send> {
    id: usize,
    cells: [Cell<T>; SEGMENT_SIZE],
    next: Atomic<Segment<T>>,
}

impl<T: Send> Segment<T> {
    fn new(id: usize) -> Self {
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
        write!(f, "Segment {{ id: {} }}", self.id)
    }
}

#[derive(Debug)]
struct Global<T: Send> {
    segments: Atomic<Segment<T>>,
    head: AtomicUsize,
    tail: AtomicUsize,
    registries: helpers::Tracker<Registry<T>>,
}

#[derive(Debug)]
pub struct Queue<T: Send>(Arc<Global<T>>);

#[derive(Debug)]
pub struct Handle<T: Send> {
    global: Arc<Global<T>>,
    head: Atomic<Segment<T>>,
    tail: Atomic<Segment<T>>,
    registry: helpers::Handle<Registry<T>>,
}

impl<T: Send> Default for PushReq<T> {
    fn default() -> Self {
        Self {
            val: Atomic::null(),
            state: State::default(),
        }
    }
}

impl<T: Send> Default for Registry<T> {
    fn default() -> Self {
        Self {
            push: PushReq::default(),
            pop: PopReq::default(),
            lower_bound: ATOMIC_USIZE_INIT,
        }
    }
}

impl<T: Send> Global<T> {
    fn new() -> Self {
        Self {
            segments: Atomic::new(Segment::new(0)),
            head: ATOMIC_USIZE_INIT,
            tail: ATOMIC_USIZE_INIT,
            registries: helpers::Tracker::new(),
        }
    }
}

impl<T: Send> Queue<T> {
    pub fn new() -> Self {
        Self { 0: Arc::new(Global::new()) }
    }

    pub fn handle(&self) -> Handle<T> {
        Handle {
            global: self.0.clone(),
            head: Atomic::null(),
            tail: Atomic::null(),
            registry: self.0.registries.handle(Registry::default()),
        }
    }
}

impl<T: Send> Handle<T> {
    const PATIENCE: usize = 10;
    const TAG_EMPTY: usize = 1;
    const TAG_INVALID: usize = 2;

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
    fn find_cell<'g>(
        &'g self,
        segments: &'g Atomic<Segment<T>>,
        cell_id: usize,
        guard: &'g Guard,
    ) -> &'g Cell<T> {
        let segment_id = cell_id / SEGMENT_SIZE;

        let mut s = segments.load(Ordering::Relaxed, guard);
        let mut s_ref = unsafe { s.as_ref().unwrap() };
        for id in 0..(segment_id - s_ref.id) {
            let next = s_ref.next.load(Ordering::Relaxed, guard);
            match unsafe { next.as_ref() } {
                Some(next_ref) => {
                    s = next;
                    s_ref = next_ref;
                }
                None => {
                    let segment = Owned::new(Segment::new(id + 1));
                    let _ = s_ref
                        .next
                        .compare_and_set(Shared::null(), segment, Ordering::Relaxed, guard)
                        .map(|next| {
                            s = next;
                            s_ref = unsafe { next.as_ref().unwrap() };
                        })
                        .map_err(|(next, _)| {
                            s = next;
                            s_ref = unsafe { next.as_ref().unwrap() };
                        });
                }
            }
        }

        segments.store(s, Ordering::Relaxed);
        &s_ref.cells[cell_id % SEGMENT_SIZE]
    }

    #[inline]
    fn advance_end_for_linearizability(end: &AtomicUsize, cell_id: usize) {
        let mut e = end.load(Ordering::Relaxed);
        loop {
            if e >= cell_id {
                break;
            }
            match end.compare_exchange(e, cell_id, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(e2) => e = e2,
            }
        }
    }

    #[inline]
    fn try_to_claim_req<'g>(
        &'g self,
        state: &'g State,
        cell_id: usize,
        i: usize,
    ) -> Result<(), (usize, bool)> {
        // This should be a strong update.
        state.compare_exchange(
            cell_id,
            true,
            i,
            false,
            Ordering::Relaxed,
            Ordering::Relaxed,
        )
    }

    #[inline]
    fn push_commit<'g>(&'g self, c: &'g Cell<T>, val: Shared<'g, T>, cell_id: usize) {
        Self::advance_end_for_linearizability(&self.global.tail, cell_id + 1);
        c.val.store(val, Ordering::Relaxed);
    }

    #[inline]
    fn push_fast(&self, val: Owned<T>, guard: &Guard) -> Result<(), (Owned<T>, usize)> {
        let i = self.global.tail.fetch_add(1, Ordering::Relaxed);
        let cell = self.find_cell(&self.tail, i, guard);
        cell.val
            .compare_and_set(Shared::null(), val, Ordering::Relaxed, guard)
            .map(|_| ())
            .map_err(|(_, val)| (val, i))
    }

    #[cold]
    #[inline]
    fn push_slow(&self, val: Owned<T>, mut cell_id: usize, guard: &Guard) {
        let push = &self.registry.get().push;
        let val = val.into_ptr(guard);
        push.val.store(val, Ordering::Relaxed);
        push.state.store(cell_id, true, Ordering::Relaxed);

        let tail = self.tail.load(Ordering::Relaxed, guard);
        let original_tail = Atomic::null();
        original_tail.store(tail, Ordering::Relaxed);

        loop {
            let i = self.global.tail.fetch_add(1, Ordering::Relaxed);
            let cell = self.find_cell(&original_tail, i, guard);
            if cell.push
                .compare_and_set(
                    Shared::null(),
                    Shared::from_raw(push as *const _),
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

    pub fn try_pop(&self, guard: &Guard) -> Option<T> {
        self.normalize(guard);

        let mut cell_id = 0;
        let result = self.try_pop_fast(&mut cell_id, guard)
            .or_else(|| self.try_pop_slow(cell_id, guard));
        if result.is_some() {
            self.help_pop(guard);
        }
        result.map(|result| result.into_inner())
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
        let req = &self.registry.get().pop;
        req.id.store(cell_id, Ordering::Relaxed);
        req.state.store(cell_id, true, Ordering::Relaxed);

        self.help_pop(guard);

        let i = req.state.load(Ordering::Relaxed).0;
        let cell = self.find_cell(&self.head, i, guard);
        Self::advance_end_for_linearizability(&self.global.head, cell_id + 1);

        let result = cell.val.load(Ordering::Relaxed, guard);
        unsafe { result.as_ref().map(|_| result.into()) }
    }

    fn help_push<'g>(&'g self, cell: &'g Cell<T>, i: usize, guard: &'g Guard) -> Shared<'g, T> {
        let cell_val = match cell.val.compare_and_set(
            Shared::null(),
            Shared::null().with_tag(Self::TAG_INVALID),
            Ordering::Relaxed,
            guard,
        ) {
            Ok(_) => Shared::null().with_tag(Self::TAG_INVALID),
            Err(val) => {
                if val != Shared::null().with_tag(Self::TAG_INVALID) {
                    return val;
                }
                val
            }
        };

        let push = &self.registry.get().push;
        let (mut id, pending) = push.state.load(Ordering::Relaxed);
        let mut cell_req = cell.push.load(Ordering::Relaxed, guard);

        if cell_req == Shared::null() {
            let (peer, peer_id, peer_pending) = loop {
                let peer = self.registry.peer();
                let (peer_id, peer_pending) = peer.push.state.load(Ordering::Relaxed);

                if id == 0 || id == peer_id { break (peer, peer_id, peer_pending); }

                push.state.store(0, pending, Ordering::Relaxed);
                id = 0;
                self.registry.next();
            };

            if peer_pending &&
                peer_id <= i &&
                cell.push.compare_and_set(
                    Shared::null(),
                    Shared::from_raw(&peer.push as *const _),
                    Ordering::Relaxed,
                    guard,
                )
                .is_err() {
                push.state.store(peer_id, pending, Ordering::Relaxed);
            } else {
                self.registry.next();
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

        let push_ref = unsafe { push.as_ref().unwrap() };
        let (id, pending) = push_ref.state.load(Ordering::Relaxed);
        let v = push_ref.val.load(Ordering::Relaxed, guard);

        if peer_id > i {
            if cell_val.tag() == Self::TAG_INVALID && self.global.tail.load(Ordering::Relaxed) <= i {
                return Shared::null().with_tag(Self::TAG_EMPTY);
            }
        } else {
            if self.try_to_claim_req(&push_ref.state, peer_id, i).is_ok() ||
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
