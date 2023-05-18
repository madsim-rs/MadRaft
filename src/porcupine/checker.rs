//! Parallel linearizability checker.

use std::time::Duration;

use crate::porcupine::{CheckResult, Model, Operation};

pub(crate) struct LinearizationInfo {}

pub(super) fn check_operations<M, IT, OT>(
    model: M,
    history: Vec<Operation<IT, OT>>,
    verbose: bool,
    timeout: Option<Duration>,
) -> (CheckResult, LinearizationInfo)
where
    M: Model<In = IT, Out = OT>,
{
    let partition = <M as Model>::partition(history);
    todo!()
}
