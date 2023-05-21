//! A tool to check linearizability.
#![cfg(test)]

mod checker;
pub mod kv;
pub mod model;
mod utils;

use std::time::Duration;
use checker::LinearizationInfo;
use model::{Entry, Model, Operation};

/// Linearizability check result.
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub(crate) enum CheckResult {
    /// Ok
    Ok = 0,
    /// Timeout
    Unknown = 1,
    /// Unlinearizable
    Illegal = 2,
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
#[allow(dead_code)] // TODO support verbose
pub(crate) async fn check_operation_verbose<M: Model>(
    history: Vec<Operation<M>>,
    timeout: Duration,
) -> (CheckResult, LinearizationInfo) {
    // TODO support verbose
    checker::check_operations::<M>(history, true, Some(timeout)).await
}
