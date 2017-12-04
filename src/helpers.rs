use std::fmt;
use std::sync::Arc;

use epoch;

use list::{List, Node, Iter, IterResult};

pub struct Tracker<T: Sync> {
    list: Arc<List<T>>,
}

pub struct Handle<T: Sync> {
    _list: Arc<List<T>>,
    node: *const Node<T>,
    iter: Iter<T>,
}

impl<T: Sync> fmt::Debug for Tracker<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tracker")
    }
}

impl<T: Sync> Tracker<T> {
    pub fn new() -> Self {
        Self {
            list: Arc::new(List::new()),
        }
    }

    pub fn handle(&self, data: T, guard: &epoch::Guard) -> Option<Handle<T>> {
        let _list = self.list.clone();
        let node = unsafe { _list.insert(data, guard).as_ref().unwrap() as *const _ };
        _list.iter(guard).map(|iter| {
            Handle { _list, node, iter }
        })
    }
}

impl<T: Sync> fmt::Debug for Handle<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Handle")
    }
}


impl<T: Sync> Handle<T> {
    pub fn get(&self) -> &T {
        unsafe { (*self.node).get() }
    }

    pub fn next<'g>(&'g mut self, guard: &'g epoch::Guard) -> &'g T {
        loop {
            match self.iter.next(guard) {
                IterResult::Some(result) => return unsafe { (*result).get() },
                IterResult::None => self.iter.restart(guard),
                IterResult::Restart => continue,
            }
        }
    }
}

impl<T: Sync> Drop for Handle<T> {
    fn drop(&mut self) {
        unsafe {
            (*self.node).delete(epoch::unprotected());
        }
    }
}
