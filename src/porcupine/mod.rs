//! A lib checking linearizability.

#![deny(clippy::all)]

#![allow(dead_code)] // TODO

use std::time::Duration;

use checker::LinearizationInfo;
use model::{Model, Operation};

mod checker;
pub mod kv;
pub mod model;

/// Linearizability check result.
pub(crate) enum CheckResult {
    /// Timeout
    Unknown,
    /// Ok
    Ok,
    /// Unlinearizable
    Illegal,
}

pub(crate) fn check_operations<IT, OT>(
    model: impl Model<In = IT, Out = OT>,
    history: Vec<Operation<IT, OT>>,
) -> bool {
    let (res, _) = checker::check_operations(model, history, false, None);
    matches!(res, CheckResult::Ok)
}

pub(crate) fn check_operations_timeout<IT, OT>(
    model: impl Model<In = IT, Out = OT>,
    history: Vec<Operation<IT, OT>>,
    timeout: Duration,
) -> CheckResult {
    let (res, _) = checker::check_operations(model, history, false, Some(timeout));
    res
}

pub(crate) fn check_operation_verbose<IT, OT>(
    model: impl Model<In = IT, Out = OT>,
    history: Vec<Operation<IT, OT>>,
    timeout: Duration,
) -> (CheckResult, LinearizationInfo) {
    checker::check_operations(model, history, true, Some(timeout))
}
