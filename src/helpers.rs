use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use epoch;

use list::{List, Node};

pub struct Tracker<T: Sync> {
    collector: epoch::Collector,
    list: Arc<List<T>>,
}

pub struct Handle<T: Sync> {
    epoch: epoch::QSBRHandle,
    list: Arc<List<T>>,
    node: *const Node<T>,
    iter: UnsafeCell<*const Node<T>>,
}

impl<T: Sync> Tracker<T> {
    pub fn new() -> Self {
        Self {
            collector: epoch::Collector::new(),
            list: Arc::new(List::new()),
        }
    }

    pub fn handle(&self, data: T) -> Handle<T> {
        let epoch = self.collector.qsbr_handle();
        let list = self.list.clone();
        let node = unsafe { list.insert(data, &epoch).as_ref().unwrap() as *const _ };
        Handle {
            epoch,
            list,
            node,
            iter: UnsafeCell::new(node),
        }
    }
}

impl<T: Sync> Handle<T> {
    pub fn get(&self) -> &T {
        unsafe { (*self.node).get_data() }
    }

    pub fn peer(&self) -> &T {
        unsafe {
            let iter = *self.iter.get();

            if iter.is_null() {
                self.epoch.on_quiescent_state();
                *self.iter.get() = self.list.get_head(&self.epoch).as_raw();
            }

            (*iter).get_data()
        }
    }

    pub fn next(&self) {
        unsafe {
            let iter = *self.iter.get();

            if iter.is_null() {
                self.epoch.on_quiescent_state();
                *self.iter.get() = self.list.get_head(&self.epoch).as_raw();
            } else {
                *self.iter.get() = (*iter)
                    .get_next()
                    .load(Ordering::Relaxed, &self.epoch)
                    .as_raw();
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
