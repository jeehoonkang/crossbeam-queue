use std::fmt;
use std::sync::Arc;

use epoch;

use list::{List, Node, Iter, IterResult};

pub struct Registry<T: Sync> {
    list: Arc<List<T>>,
}

pub struct Participant<T: Sync> {
    _list: Arc<List<T>>,
    node: *const Node<T>,
    iter: Iter<T>,
}

impl<T: Sync> fmt::Debug for Registry<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Registry")
    }
}

impl<T: Sync> Registry<T> {
    pub fn new() -> Self {
        Self {
            list: Arc::new(List::new()),
        }
    }

    pub fn participant(&self, data: T, guard: &epoch::Guard) -> Option<Participant<T>> {
        let _list = self.list.clone();
        let node = unsafe { _list.insert(data, guard).as_ref().unwrap() as *const _ };
        _list.iter(guard).map(|iter| {
            Participant { _list, node, iter }
        })
    }
}

impl<T: Sync> fmt::Debug for Participant<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Participant")
    }
}


impl<T: Sync> Participant<T> {
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

impl<T: Sync> Drop for Participant<T> {
    fn drop(&mut self) {
        unsafe {
            (*self.node).delete(epoch::unprotected());
        }
    }
}
