//! Parallel linearizability checker.

use crate::porcupine::{
    model::EntryValue,
    utils::{EntryNode, LinkedEntries},
    CheckResult, Entry, Model, Operation,
};
use bit_vec::BitVec;
use futures::{stream::FuturesUnordered, StreamExt};
use madsim::time;
use std::{collections::HashMap, mem, time::Duration};

pub(crate) struct LinearizationInfo {}

fn cache_contains<M: Model>(cache: &HashMap<BitVec, Vec<M>>, bv: &BitVec, m: &M) -> bool {
    if let Some(entries) = cache.get(bv) {
        return entries.contains(m);
    }
    return false;
}

struct CallEntry<M: Model> {
    call: Box<EntryNode<M::In, M::Out>>,
    ret: Box<EntryNode<M::In, M::Out>>,
    state: M,
}

/// Check single sub-history. Return Some() if it's linearizable.
fn check_single<M: Model>(history: Vec<Entry<M>>, _verbose: bool) -> Option<()> {
    let n = history.len() / 2; // number of operations

    // calls considered done
    let mut linearized = BitVec::with_capacity(n); // call set
    let mut cache = HashMap::<BitVec, Vec<M>>::new(); // call set -> state
    let mut calls: Vec<CallEntry<M>> = vec![]; // sort in time
    let undecided = LinkedEntries::from(history);

    // cursor
    let mut entry = undecided.front_mut().unwrap();

    let mut state = M::init();

    while !undecided.is_empty() {
        if matches!(entry.value, EntryValue::Call(_)) {
            // the matched return entry
            let matched = entry.matched_mut().unwrap();
            let (ok, new_state) = state.step(entry.unwrap_in(), matched.unwrap_out());
            if ok {
                let mut new_linearized = linearized.clone();
                new_linearized.set(entry.id, true);
                if !cache_contains(&cache, &new_linearized, &new_state) {
                    linearized.set(entry.id, true);
                    cache
                        .get_mut(&new_linearized)
                        .unwrap()
                        .push(new_state.clone());
                    let (call, ret) = entry.lift();
                    calls.push(CallEntry {
                        call,
                        ret,
                        state: mem::replace(&mut state, new_state),
                    });
                    entry = undecided.front_mut().unwrap();
                } else {
                    // this state is visited before
                    entry = entry.next_mut().unwrap();
                }
            } else {
                entry = entry.next_mut().unwrap();
            }
        } else {
            // back track
            let CallEntry {
                call,
                ret,
                state: state0,
            } = calls.pop()?;
            state = state0;
            linearized.set(call.id as _, false);
            call.unlift(ret);
            entry = entry.next_mut().unwrap();
        }
    }
    Some(())
}

/// Check history in parallel.
///
/// For each sub-history, spawn a task to test its linearizability.
async fn check_parallel<M: Model>(
    histories: Vec<Vec<Entry<M>>>,
    verbose: bool,
) -> (CheckResult, LinearizationInfo) {
    let mut futures: FuturesUnordered<_> = histories
        .into_iter()
        .map(|subhistory| async move { check_single::<M>(subhistory, verbose) })
        .collect();
    let mut check_result = CheckResult::Ok;
    while let Some(res) = futures.next().await {
        if res.is_none() {
            check_result = CheckResult::Illegal;
            if !verbose {
                break; // collect linearizable prefix under verbose mode
            }
        }
    }
    // TODO support verbose print
    (check_result, LinearizationInfo {})
}

pub(super) async fn check_operations<M: Model>(
    history: Vec<Operation<M>>,
    verbose: bool,
    timeout: Option<Duration>,
) -> (CheckResult, LinearizationInfo) {
    let histories = <M as Model>::partition(history);
    if let Some(dur) = timeout {
        // XXX I'm not sure
        match time::timeout(dur, check_parallel::<M>(histories, verbose)).await {
            Ok(v) => v,
            Err(_) => (CheckResult::Unknown, LinearizationInfo {}),
        }
    } else {
        check_parallel::<M>(histories, verbose).await
    }
}