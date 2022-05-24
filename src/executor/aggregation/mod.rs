// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::ArrayImpl;
use crate::types::DataValue;

mod count;
mod dp_count;
mod dp_sum;
mod min_max;
mod rowcount;
mod sum;

pub use count::*;
pub use dp_count::*;
pub use dp_sum::*;
pub use min_max::*;
pub use rowcount::*;
pub use sum::*;

/// `AggregationState` records the state of an aggregation
pub trait AggregationState: 'static + Send + Sync {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError>;

    fn update_single(&mut self, value: &DataValue) -> Result<(), ExecutorError>;

    fn output(&self) -> DataValue;
}
