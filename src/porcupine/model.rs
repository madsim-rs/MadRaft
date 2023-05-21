//! History model.

use std::fmt::Debug;

/// Operation
#[derive(Debug, Clone)]
pub(crate) struct Operation<M: Model> {
    /// optional, unless you want a visualization
    pub client_id: Option<usize>,
    /// input value
    pub input: M::In,
    /// invocation time
    pub call: u128,
    /// output value
    pub output: M::Out,
    /// response time
    pub ret: u128,
}

/// Entry type, could be call or return.
#[derive(Debug, Default)]
pub(crate) enum EntryValue<In, Out> {
    #[default]
    Null, // only used in sentinel node
    Call(In),
    Return(Out),
}

/// Entry
#[derive(Debug)]
pub(crate) struct Entry<M: Model> {
    pub value: EntryValue<M::In, M::Out>,
    pub id: usize,
    pub time: u128,
    #[allow(dead_code)] // used in verbose mode
    pub client_id: Option<usize>,
}

/// Model. Eq trait needs to be implemented to represent equality on states.
pub(crate) trait Model: Eq + Clone + Debug {
    /// Input type
    type In: Clone + Debug;

    /// Output type
    type Out: Clone + Debug;

    /// Partition operations, such that a history is linearizable if and only if
    /// each partition is linearizable.
    ///
    /// Each partition should be sorted by time. If two entries are of the same time,
    /// calls should always be placed before returns.
    fn partition(history: Vec<Operation<Self>>) -> Vec<Vec<Entry<Self>>>;

    /// Initial state of the system.
    fn init() -> Self;

    /// Step functions for the system.
    ///
    /// Returns whether or not the system could take this step with the given
    /// inputs and outputs, and the new state. This should not mutate the
    /// existing state.
    fn step(&self, input: &Self::In, output: &Self::Out) -> (bool, Self);
}
