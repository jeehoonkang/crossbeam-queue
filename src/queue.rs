use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering;
use arrayvec::ArrayVec;
use epoch::{Atomic, Ptr, Owned, Scope};

use list;
use helpers;

const SEGMENT_SIZE: usize = 64;

struct State(AtomicUsize);

impl State {
    pub fn new() -> Self {
        Self { 0: ATOMIC_USIZE_INIT }
    }

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

struct PushReq<T: Send> {
    val: Atomic<T>,
    state: State,
}

unsafe impl<T: Send> Sync for PushReq<T> {}

struct PopReq {
    id: AtomicUsize,
    state: State,
}

struct Cell<T: Send> {
    val: Atomic<T>,
    push: Atomic<PushReq<T>>,
    pop: Atomic<PopReq>,
}

struct Segment<T: Send> {
    id: usize,
    cells: ArrayVec<[Cell<T>; SEGMENT_SIZE]>,
}

struct Global<T: Send> {
    segments: list::List<Segment<T>>, // FIXME: maybe we don't need it..
    head: AtomicUsize,
    tail: AtomicUsize,
    push_reqs: helpers::Tracker<PushReq<T>>,
    pop_reqs: helpers::Tracker<PopReq>,
}

pub struct Queue<T: Send>(Arc<Global<T>>);

pub struct Handle<T: Send> {
    global: Arc<Global<T>>,
    head: Atomic<list::Node<Segment<T>>>,
    tail: Atomic<list::Node<Segment<T>>>,
    push_req: helpers::Handle<PushReq<T>>,
    pop_req: helpers::Handle<PopReq>,
}

impl<T: Send> PushReq<T> {
    fn new() -> Self {
        PushReq {
            val: Atomic::null(),
            state: State::new(),
        }
    }
}

impl PopReq {
    fn new() -> Self {
        PopReq {
            id: ATOMIC_USIZE_INIT,
            state: State::new(),
        }
    }
}

impl<T: Send> Cell<T> {
    fn new() -> Self {
        unsafe { mem::uninitialized() }
    }
}

impl<T: Send> Segment<T> {
    fn new(id: usize) -> Self {
        Segment {
            id: id,
            cells: ArrayVec::new(),
        }
    }
}

impl<T: Send> Global<T> {
    pub fn new() -> Self {
        Self {
            segments: list::List::new(),
            head: ATOMIC_USIZE_INIT,
            tail: ATOMIC_USIZE_INIT,
            push_reqs: helpers::Tracker::new(),
            pop_reqs: helpers::Tracker::new(),
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
            push_req: self.0.push_reqs.handle(PushReq::new()),
            pop_req: self.0.pop_reqs.handle(PopReq::new()),
        }
    }
}

impl<T: Send> Handle<T> {
    const PATIENCE: usize = 10;

    #[inline]
    #[allow(unused_variables)]
    fn find_cell<'scope>(
        &'scope self,
        segment: &'scope Atomic<list::Node<Segment<T>>>,
        cell_id: usize,
        scope: &'scope Scope,
    ) -> Ptr<'scope, Cell<T>> {
        let segment_id = cell_id / SEGMENT_SIZE;

        let mut s = segment.load(Ordering::Relaxed, scope);
        let mut s_ref = unsafe { s.as_ref().unwrap() };
        for _ in 0..(segment_id - s_ref.get_data().id) {
            let next = s_ref.get_next().load(Ordering::Relaxed, scope);
            match unsafe { next.as_ref() } {
                Some(next_ref) => {
                    s = next;
                    s_ref = next_ref;
                }
                None => {
                    unimplemented!();
                }
            }
        }

        segment.store(s, Ordering::Relaxed);
        Ptr::from_raw(&s_ref.get_data().cells[cell_id % SEGMENT_SIZE] as *const _)
    }

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
        state.compare_exchange(
            cell_id,
            true,
            i,
            false,
            Ordering::Relaxed,
            Ordering::Relaxed,
        )
    }

    pub fn push(&self, val: T, scope: &Scope) {
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

    #[inline]
    fn push_commit<'scope>(&'scope self, c: &'scope Cell<T>, val: Ptr<'scope, T>, cell_id: usize) {
        Self::advance_end_for_linearizability(&self.global.tail, cell_id + 1);
        c.val.store(val, Ordering::Relaxed);
    }

    #[inline]
    fn push_fast(&self, val: Owned<T>, scope: &Scope) -> Result<(), (Owned<T>, usize)> {
        let i = self.global.tail.fetch_add(1, Ordering::Relaxed);
        let c = self.find_cell(&self.tail, i, scope);
        unsafe {
            c.as_ref()
                .unwrap()
                .val
                .compare_and_set_owned(Ptr::null(), val, Ordering::Relaxed, scope)
                .map(|_| ())
                .map_err(|(_, val)| (val, i))
        }
    }

    #[cold]
    #[inline]
    fn push_slow(&self, val: Owned<T>, mut cell_id: usize, scope: &Scope) {
        let req = self.push_req.get();
        let val = val.into_ptr(scope);
        req.val.store(val, Ordering::Relaxed);
        req.state.store(cell_id, true, Ordering::Release);

        let tail = self.tail.load(Ordering::Relaxed, scope);
        let original_tail = Atomic::null();
        original_tail.store(tail, Ordering::Relaxed);

        loop {
            let i = self.global.tail.fetch_add(1, Ordering::Relaxed);
            let c = self.find_cell(&original_tail, i, scope);
            let c = unsafe { c.as_ref().unwrap() };
            if c.push
                .compare_and_set(
                    Ptr::null(),
                    Ptr::from_raw(req as *const _),
                    Ordering::Relaxed,
                    scope,
                )
                .is_ok() && c.val.load(Ordering::Relaxed, scope).is_null()
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

        let c = self.find_cell(&self.tail, cell_id, scope);
        let c = unsafe { c.as_ref().unwrap() };
        self.push_commit(c, val, cell_id);
    }
}
