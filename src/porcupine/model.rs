//! History model.

/// Operation
pub(crate) struct Operation<In, Out> {
    /// optional, unless you want a visualization
    pub client_id: Option<usize>,
    pub input: In,
    /// invocation time
    pub call: u64,
    pub output: Out,
    /// response time
    pub ret: u64,
}

/// Model.
///
/// - Eq trait needs to be implemented to represent equality on states.
pub(crate) trait Model {
    /// Input type
    type In;

    /// Output type
    type Out;

    /// Partition operations, such that a history is linearizable if and only if
    /// each partition is linearzable.
    fn partition(
        history: Vec<Operation<Self::In, Self::Out>>,
    ) -> Vec<Vec<Operation<Self::In, Self::Out>>>;

    /// Initial state of the system.
    fn init() -> Self;

    /// Step functions for the system.
    ///
    /// Returns whether or not the system could take this step with the given
    /// inputs and outputs.
    fn step(self, input: Self::In, output: Self::Out) -> (bool, Self);

    /// Equality on states.
    fn equal(&self, other: &Self) -> bool;
}
