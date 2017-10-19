use std::fmt;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering;
use epoch::{Atomic, Ptr, Owned, Scope};

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
    push_req: PushReq<T>,
    pop_req: PopReq,
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
            push_req: PushReq::default(),
            pop_req: PopReq::default(),
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
    fn normalize(&self, scope: &Scope) {
        let segments = self.global.segments.load(Ordering::Relaxed, scope);
        let segments_ref = unsafe { segments.as_ref().unwrap() };

        let tail = self.tail.load(Ordering::Relaxed, scope);
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

        let head = self.head.load(Ordering::Relaxed, scope);
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
    fn find_cell<'scope>(
        &'scope self,
        segments: &'scope Atomic<Segment<T>>,
        cell_id: usize,
        scope: &'scope Scope,
    ) -> &'scope Cell<T> {
        let segment_id = cell_id / SEGMENT_SIZE;

        let mut s = segments.load(Ordering::Relaxed, scope);
        let mut s_ref = unsafe { s.as_ref().unwrap() };
        for id in 0..(segment_id - s_ref.id) {
            let next = s_ref.next.load(Ordering::Relaxed, scope);
            match unsafe { next.as_ref() } {
                Some(next_ref) => {
                    s = next;
                    s_ref = next_ref;
                }
                None => {
                    let segment = Owned::new(Segment::new(id + 1));
                    let _ = s_ref
                        .next
                        .compare_and_set_owned(Ptr::null(), segment, Ordering::Relaxed, scope)
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
    fn try_to_claim_req<'scope>(
        &'scope self,
        state: &'scope State,
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
    fn push_commit<'scope>(&'scope self, c: &'scope Cell<T>, val: Ptr<'scope, T>, cell_id: usize) {
        Self::advance_end_for_linearizability(&self.global.tail, cell_id + 1);
        c.val.store(val, Ordering::Relaxed);
    }

    #[inline]
    fn push_fast(&self, val: Owned<T>, scope: &Scope) -> Result<(), (Owned<T>, usize)> {
        let i = self.global.tail.fetch_add(1, Ordering::Relaxed);
        let cell = self.find_cell(&self.tail, i, scope);
        cell.val
            .compare_and_set_owned(Ptr::null(), val, Ordering::Relaxed, scope)
            .map(|_| ())
            .map_err(|(_, val)| (val, i))
    }

    #[cold]
    #[inline]
    fn push_slow(&self, val: Owned<T>, mut cell_id: usize, scope: &Scope) {
        let req = &self.registry.get().push_req;
        let val = val.into_ptr(scope);
        req.val.store(val, Ordering::Relaxed);
        req.state.store(cell_id, true, Ordering::Relaxed);

        let tail = self.tail.load(Ordering::Relaxed, scope);
        let original_tail = Atomic::null();
        original_tail.store(tail, Ordering::Relaxed);

        loop {
            let i = self.global.tail.fetch_add(1, Ordering::Relaxed);
            let cell = self.find_cell(&original_tail, i, scope);
            if cell.push
                .compare_and_set(
                    Ptr::null(),
                    Ptr::from_raw(req as *const _),
                    Ordering::Relaxed,
                    scope,
                )
                .is_ok() && cell.val.load(Ordering::Relaxed, scope).is_null()
            {
                let _ = self.try_to_claim_req(&req.state, cell_id, i);
                break;
            }

            let (id, pending) = req.state.load(Ordering::Relaxed);
            cell_id = id;
            if !pending {
                break;
            }
        }

        let cell = self.find_cell(&self.tail, cell_id, scope);
        self.push_commit(cell, val, cell_id);
    }

    pub fn push(&self, val: T, scope: &Scope) {
        self.normalize(scope);

        let mut val = Owned::new(val);
        let mut cell_id = 0;
        for _ in 0..Self::PATIENCE {
            match self.push_fast(val, scope) {
                Ok(_) => return,
                Err((v, id)) => {
                    val = v;
                    cell_id = id;
                }
            }
        }

        self.push_slow(val, cell_id, scope);
    }

    pub fn try_pop(&self, scope: &Scope) -> Option<T> {
        self.normalize(scope);

        let mut cell_id = 0;
        let result = self.try_pop_fast(&mut cell_id, scope)
            .or_else(|| self.try_pop_slow(cell_id, scope));
        if result.is_some() {
            self.help_pop(scope);
        }
        result.map(|result| result.into_inner())
    }

    #[inline]
    fn try_pop_fast<'scope>(&'scope self, id: &'scope mut usize, scope: &'scope Scope) -> Option<Owned<T>> {
        for _ in 0..Self::PATIENCE {
            let i = self.global.head.fetch_add(1, Ordering::Relaxed);
            let cell = self.find_cell(&self.head, i, scope);
            let result = self.help_push(cell, i, scope);

            match result.tag() {
                Self::TAG_EMPTY => return None,
                Self::TAG_INVALID => *id = i,
                _ => {
                    if cell.pop
                        .compare_and_set(
                            Ptr::null(),
                            Ptr::null().with_tag(Self::TAG_INVALID),
                            Ordering::Relaxed,
                            scope,
                        )
                        .is_ok()
                    {
                        return Some(unsafe { result.into_owned() });
                    } else {
                        *id = i;
                    }
                }
            }
        }
        None
    }

    #[inline]
    fn try_pop_slow(&self, cell_id: usize, scope: &Scope) -> Option<Owned<T>> {
        let req = &self.registry.get().pop_req;
        req.id.store(cell_id, Ordering::Relaxed);
        req.state.store(cell_id, true, Ordering::Relaxed);

        self.help_pop(scope);

        let i = req.state.load(Ordering::Relaxed).0;
        let cell = self.find_cell(&self.head, i, scope);
        Self::advance_end_for_linearizability(&self.global.head, cell_id + 1);

        let result = cell.val.load(Ordering::Relaxed, scope);
        unsafe { result.as_ref().map(|_| result.into_owned()) }
    }

    fn help_push<'scope>(&'scope self, cell: &'scope Cell<T>, i: usize, scope: &'scope Scope) -> Ptr<'scope, T> {
        let val = match cell.val.compare_and_set(
            Ptr::null(),
            Ptr::null().with_tag(Self::TAG_INVALID),
            Ordering::Relaxed,
            scope,
        ) {
            Ok(_) => Ptr::null().with_tag(Self::TAG_INVALID),
            Err(val) => {
                if val != Ptr::null().with_tag(Self::TAG_INVALID) {
                    return val;
                }
                val
            }
        };

        let req = cell.push.load(Ordering::Relaxed, scope);

        if req == Ptr::null() {
            unimplemented!();
        }

        if req.tag() == Self::TAG_INVALID {
            let tail = self.global.tail.load(Ordering::Relaxed);
            return Ptr::null().with_tag(if tail <= i { Self::TAG_EMPTY } else { Self::TAG_INVALID });
        }

        let req_ref = unsafe { req.as_ref().unwrap() };
        let (id, pending) = req_ref.state.load(Ordering::Relaxed);
        let v = req_ref.val.load(Ordering::Relaxed, scope);

        if id > i {
            if val.tag() == Self::TAG_INVALID && self.global.tail.load(Ordering::Relaxed) <= i {
                return Ptr::null().with_tag(Self::TAG_EMPTY);
            }
        } else {
            if self.try_to_claim_req(&req_ref.state, id, i).is_ok() ||
                ((id, pending) == (i, false) && val.tag() == Self::TAG_INVALID) {
                self.push_commit(cell, v, i);
            }
        }
        val
    }

    #[inline]
    fn help_pop(&self, scope: &Scope) {
        unimplemented!()
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
                        let handle = collector.handle();
                        let queue = queue.handle();
                        for _ in 0..COUNT {
                            handle.pin(|scope| { queue.push(42, scope); });
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
