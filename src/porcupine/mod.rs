//! A tool to check linearizability.
#![cfg(test)]

mod checker;
pub mod kv;
pub mod model;
mod utils;

use checker::LinearizationInfo;
use model::{Entry, Model, Operation};
use std::fmt;

#[derive(Debug)]
pub(crate) enum Error {
    Illegal(LinearizationInfo),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Illegal(info) => f.write_fmt(format_args!("Illegal result: {}", info)),
        }
    }
}

impl std::error::Error for Error {}

pub(crate) type Result<T> = std::result::Result<T, Error>;

/// If this operation times out, then a false positive is possible.
pub(crate) fn check_operations<M: Model>(history: Vec<Operation<M>>) -> Result<()> {
    checker::check_operations::<M>(history, false)
}

/// If this operation times out, then a false positive is possible.
#[allow(dead_code)] // TODO support verbose
pub(crate) fn check_operation_verbose<M: Model>(history: Vec<Operation<M>>) -> Result<()> {
    checker::check_operations::<M>(history, true)
}
