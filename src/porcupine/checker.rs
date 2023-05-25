//! Parallel linearizability checker.

use crate::porcupine::{
    model::EntryValue,
    utils::{EntryNode, LinkedEntries},
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
    call: Box<EntryNode<M::In, M::Out>>,
    ret: Box<EntryNode<M::In, M::Out>>,
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
    let mut entry = undecided.front_mut().unwrap();
    let mut state = M::init();

    while !undecided.is_empty() {
        if matches!(entry.value, EntryValue::Call(_)) {
            debug!("id={} call", entry.id);
            // the matched return entry
            let matched = entry.matched_mut().unwrap();
            let (ok, new_state) = state.step(entry.unwrap_in(), matched.unwrap_out());
            if ok {
                let mut new_linearized = linearized.clone();
                new_linearized.set(entry.id, true);
                if !cache_contains(&cache, &new_linearized, &new_state) {
                    debug!("cache miss, push {} into calls", entry.id);
                    linearized.set(entry.id, true);
                    cache
                        .entry(new_linearized)
                        .or_default()
                        .push(new_state.clone());
                    let (call, ret) = entry.lift();
                    calls.push(CallEntry {
                        call,
                        ret,
                        state: mem::replace(&mut state, new_state),
                    });
                    if let Some(front) = undecided.front_mut() {
                        entry = front;
                    } else {
                        break;
                    }
                } else {
                    // this state is visited before
                    entry = entry.next_mut().unwrap();
                }
            } else {
                // call entry has next
                entry = entry.next_mut().unwrap();
            }
        } else {
            // an undecided return found, meaning that a call considered done before this
            // time point has to be revoked.
            debug!("id={} return", entry.id);
            if calls.is_empty() {
                return Err(Error::Illegal(LinearizationInfo {}));
            }
            let CallEntry {
                mut call,
                ret,
                state: state0,
            } = calls.pop().unwrap();
            debug!("revoke call {}", call.id);
            state = state0;
            linearized.set(call.id as _, false);
            entry = call.ref_mut();
            call.unlift(ret);
            // call entry has next
            entry = entry.next_mut().unwrap();
        }
    }
    Ok(())
}

pub(super) fn check_operations<M: Model>(history: Vec<Operation<M>>, verbose: bool) -> Result<()> {
    let histories = <M as Model>::partition(history);
    for history in histories {
        // TODO get linearized prefix under verbose mode
        check_single(history, verbose)?;
    }
    Ok(())
}
