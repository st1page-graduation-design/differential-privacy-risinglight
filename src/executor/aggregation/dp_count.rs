// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

// use crate::array::ArrayImplValidExt;

use probability::distribution::{Laplace, Sample};
use probability::source;

use super::*;
use crate::array::ArrayImplValidExt;
/// State for row count aggregation
pub struct DPCountAggregationState {
    result: i64,
    epsilon: f64,
}

impl DPCountAggregationState {
    pub fn new(epsilon: f64) -> Self {
        Self { result: 0, epsilon }
    }
}

impl AggregationState for DPCountAggregationState {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError> {
        // let temp = array.len() as i64;
        let temp = array.get_valid_bitmap().count_ones() as i64;
        self.result += temp;
        Ok(())
    }

    fn update_single(&mut self, _: &DataValue) -> Result<(), ExecutorError> {
        self.result += 1;
        Ok(())
    }

    fn output(&self) -> DataValue {
        let mut source = source::default();

        let lap = Laplace::new(0.0, 1.0 / self.epsilon);
        let noise = lap.sample(&mut source);
        DataValue::Float64(self.result as f64 + noise)
    }
}
