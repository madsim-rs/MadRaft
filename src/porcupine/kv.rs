//! A key-value model.

use crate::porcupine::model::{Model, Operation};

#[derive(Debug, Clone, Copy)]
pub(crate) enum KvOp {
    Get,
    Put,
    Append,
}

#[derive(Debug)]
pub(crate) struct KvInput {
    op: KvOp,
    key: String,
    value: String,
}

#[derive(Debug)]
pub(crate) struct KvOutput {
    value: String,
}

pub(crate) struct KvModel {
    state: String,
}

impl Model for KvModel {
    type In = KvInput;
    type Out = KvOutput;

    fn partition(
        history: Vec<Operation<KvInput, KvOutput>>,
    ) -> Vec<Vec<Operation<KvInput, KvOutput>>> {
        todo!("partition by key")
    }

    fn init() -> Self {
        KvModel {
            state: "".to_string(),
        }
    }

    fn step(self, input: KvInput, output: KvOutput) -> (bool, Self) {
        match input.op {
            KvOp::Get => (output.value == self.state, self),
            KvOp::Put => (true, Self { state: input.value }),
            KvOp::Append => (
                true,
                Self {
                    state: self.state + input.value.as_str(),
                },
            ),
        }
    }

    fn equal(&self, other: &Self) -> bool {
        self.state == other.state
    }
}
