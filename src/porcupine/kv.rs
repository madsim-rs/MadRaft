//! A key-value model.

use crate::porcupine::model::{Entry, EntryValue, Model, Operation};
use std::{cmp::Ordering, collections::HashMap};

#[derive(Debug, Clone, Copy)]
pub(crate) enum KvOp {
    Get,
    Put,
    Append,
}

#[derive(Debug, Clone)]
pub(crate) struct KvInput {
    pub(crate) op: KvOp,
    pub(crate) key: String,
    pub(crate) value: String,
}

#[derive(Debug, Clone)]
pub(crate) struct KvOutput {
    pub(crate) value: String,
}

/// Model for single-version key-value store.
///
/// A single instance of `KvModel` indicates the state of a single entry
/// in the KV store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvModel {
    state: String,
}

impl Model for KvModel {
    type In = KvInput;
    type Out = KvOutput;

    // partition the history by key, and then sort by time
    fn partition(history: Vec<Operation<Self>>) -> Vec<Vec<Entry<Self>>> {
        // key -> history of a single key
        let mut map = HashMap::<String, (usize, Vec<Entry<Self>>)>::new();

        for op in history {
            let key = op.input.key.clone();
            let (id, key_hist) = map.entry(key).or_default();

            // turn operation into a call entry and a return entry
            let call_entry = Entry {
                value: EntryValue::Call::<KvInput, KvOutput>(op.input),
                id: *id,
                time: op.call,
                client_id: op.client_id,
            };
            let return_entry = Entry {
                value: EntryValue::Return::<KvInput, KvOutput>(op.output),
                id: *id,
                time: op.ret,
                client_id: op.client_id,
            };

            *id += 1;
            key_hist.push(call_entry);
            key_hist.push(return_entry);
        }

        map.into_values()
            .map(|(_, mut v)| {
                // sort by time and then entry type
                v.sort_by(|x, y| {
                    x.time.cmp(&y.time).then_with(|| {
                        if matches!(x.value, EntryValue::Call(_)) {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    })
                });
                v
            })
            .collect()
    }

    fn init() -> Self {
        KvModel {
            state: "".to_string(),
        }
    }

    fn step(&self, input: &KvInput, output: &KvOutput) -> (bool, Self) {
        match input.op {
            KvOp::Get => (output.value == self.state, self.to_owned()),
            KvOp::Put => (
                true,
                Self {
                    state: input.value.clone(),
                },
            ),
            KvOp::Append => (
                true,
                Self {
                    state: self.state.to_owned() + input.value.as_str(),
                },
            ),
        }
    }
}
