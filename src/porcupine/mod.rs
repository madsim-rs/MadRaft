//! A lib checking linearizability.
#![cfg(test)]
#![deny(clippy::all)]

use std::time::Duration;

use checker::LinearizationInfo;
use model::{Entry, Model, Operation};

mod checker;
pub mod kv;
pub mod model;
mod utils;

/// Linearizability check result.
pub(crate) enum CheckResult {
    /// Timeout
    Unknown,
    /// Ok
    Ok,
    /// Unlinearizable
    Illegal,
}

/// If this operation times out, then a false positive is possible.
pub(crate) async fn check_operations_timeout<M: Model>(
    history: Vec<Operation<M>>,
    timeout: Duration,
) -> CheckResult {
    let (res, _) = checker::check_operations::<M>(history, false, Some(timeout)).await;
    res
}

/// If this operation times out, then a false positive is possible.
pub(crate) async fn check_operation_verbose<M: Model>(
    history: Vec<Operation<M>>,
    timeout: Duration,
) -> (CheckResult, LinearizationInfo) {
    // TODO support verbose
    checker::check_operations::<M>(history, true, Some(timeout)).await
}
