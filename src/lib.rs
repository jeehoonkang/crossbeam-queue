//! Yang et al. A Wait-free Queue as Fast as Fetch-and-Add. PPoPP 2016.
//! http://dl.acm.org/citation.cfm?id=2851168

extern crate core;
extern crate crossbeam_epoch as epoch;
extern crate crossbeam_utils;

pub mod list;
// pub mod helpers;
// pub mod queue;
