//! Parallel linearizability checker.

use crate::porcupine::{
    model::EntryValue,
    utils::{EntryNode, EntryView, LinkedEntries},
    Entry, Error, Model, Operation, Result,
};
use bit_vec::BitVec;
use std::{collections::HashMap, fmt, mem};

#[derive(Debug)]
pub(crate) struct LinearizationInfo {}

impl fmt::Display for LinearizationInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LinearizationInfo")
    }
}

fn cache_contains<M: Model>(cache: &HashMap<BitVec, Vec<M>>, bv: &BitVec, m: &M) -> bool {
    if let Some(entries) = cache.get(bv) {
        return entries.contains(m);
    }
    false
}

struct CallEntry<M: Model> {
    call: EntryView<M::In, M::Out>,
    state: M,
}

/// Check single sub-history. Return Some() if it's linearizable.
fn check_single<M: Model>(history: Vec<Entry<M>>, _verbose: bool) -> Result<()> {
    let n = history.len() / 2; // number of operations
    debug!("history {:?}", history);

    let mut linearized = BitVec::from_elem(n, false); // call set
    let mut cache = HashMap::<BitVec, Vec<M>>::new(); // call set -> state
    let mut calls: Vec<CallEntry<M>> = vec![]; // sort in time
    let undecided = LinkedEntries::from(history);

    // cursor
    let mut entry = undecided.front().unwrap();
    let mut state = M::init();

    while !undecided.is_empty() {
        if matches!(*entry.borrow().value, EntryValue::Call(_)) {
            debug!("id={} call", entry.borrow().id);
            // the matched return entry
            let matched = entry.borrow().matched().unwrap();
            let (ok, new_state) =
                state.step(entry.borrow().unwrap_in(), matched.borrow().unwrap_out());
            if ok {
                let mut new_linearized = linearized.clone();
                new_linearized.set(entry.borrow().id, true);
                if !cache_contains(&cache, &new_linearized, &new_state) {
                    debug!("cache miss, push {} into calls", entry.borrow().id);
                    linearized.set(entry.borrow().id, true);
                    cache
                        .entry(new_linearized)
                        .or_default()
                        .push(new_state.clone());
                    let call = entry.borrow().lift();
                    calls.push(CallEntry {
                        call,
                        state: mem::replace(&mut state, new_state),
                    });
                    if let Some(front) = undecided.front() {
                        entry = front;
                    } else {
                        break;
                    }
                } else {
                    // this state is visited before
                    entry = EntryNode::next(entry).unwrap();
                }
            } else {
                // call entry has next
                entry = EntryNode::next(entry).unwrap();
            }
        } else {
            // an undecided return found, meaning that a call considered done before this
            // time point has to be revoked.
            debug!("id={} return", entry.borrow().id);
            if calls.is_empty() {
                return Err(Error::Illegal(LinearizationInfo {}));
            }
            let CallEntry {
                call,
                state: state0,
            } = calls.pop().unwrap();
            debug!("revoke call {}", call.id);
            state = state0;
            linearized.set(call.id as _, false);
            // entry = call.get_ref();
            entry = call.unlift();
            // call entry has next
            entry = EntryNode::next(entry).unwrap();
        }
    }
    Ok(())
}

pub(super) fn check_operations<M: Model>(history: Vec<Operation<M>>, verbose: bool) -> Result<()> {
    let histories = <M as Model>::partition(history);
    for history in histories {
        check_single(history, verbose)?;
    }
    Ok(())
}
