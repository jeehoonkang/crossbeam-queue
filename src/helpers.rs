use std::fmt;
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use epoch;

use list::{List, Node};

pub struct Tracker<T: Sync> {
    collector: epoch::Collector,
    list: Arc<List<T>>,
}

pub struct Handle<T: Sync> {
    epoch: epoch::Guard,
    list: Arc<List<T>>,
    node: *const Node<T>,
    iter: Cell<*const Node<T>>,
}

impl<T: Sync> fmt::Debug for Tracker<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tracker")
    }
}

impl<T: Sync> Tracker<T> {
    pub fn new() -> Self {
        Self {
            collector: epoch::Collector::new(),
            list: Arc::new(List::new()),
        }
    }

    pub fn handle(&self, data: T) -> Handle<T> {
        let epoch = self.collector.guard();
        let list = self.list.clone();
        let node = unsafe { list.insert(data, &epoch).as_ref().unwrap() as *const _ };
        Handle {
            epoch,
            list,
            node,
            iter: Cell::new(node),
        }
    }
}

impl<T: Sync> fmt::Debug for Handle<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Handle")
    }
}


impl<T: Sync> Handle<T> {
    pub fn get(&self) -> &T {
        unsafe { (*self.node).get_data() }
    }

    pub fn peer(&self) -> &T {
        unsafe { (*self.iter.get()).get_data() }
    }

    pub fn next(&self) {
        unsafe {
            let iter = self.iter.get();
            let next = (*iter).get_next().load(Ordering::Relaxed, &self.epoch);
            match next.as_ref() {
                None => {
                    self.epoch.safepoint();
                    self.iter.set(self.list.get_head(&self.epoch).as_raw());
                }
                Some(next) => {
                    self.iter.set(next);
                }
            }
        }
    }
}

impl<T: Sync> Drop for Handle<T> {
    fn drop(&mut self) {
        unsafe {
            (*self.node).delete(&self.epoch);
        }
    }
}
