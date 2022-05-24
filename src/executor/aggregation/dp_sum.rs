// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

// use crate::array::ArrayImplValidExt;

use std::cmp::max;

use float_ord::FloatOrd;
use num_traits::ToPrimitive;
use probability::distribution::{Laplace, Sample};
use probability::source;

use super::*;
/// State for row count aggregation
pub struct DPSumAggregationState {
    sum: f64,
    max: f64,
    epsilon: f64,
}

impl DPSumAggregationState {
    pub fn new(epsilon: f64) -> Self {
        Self {
            sum: 0.0,
            epsilon,
            max: 0.0,
        }
    }
}

impl AggregationState for DPSumAggregationState {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError> {
        for i in 0..array.len() {
            self.update_single(&array.get(i))?;
        }
        Ok(())
    }

    fn update_single(&mut self, v: &DataValue) -> Result<(), ExecutorError> {
        let v = match v {
            DataValue::Int32(v) => *v as f64,
            DataValue::Int64(v) => *v as f64,
            DataValue::Float64(v) => *v,
            DataValue::Decimal(v) => v.to_f64().unwrap(),
            _ => panic!(),
        };
        self.max = max(FloatOrd::<f64>(self.max), FloatOrd::<f64>(v)).0;
        self.sum = self.sum + v;
        Ok(())
    }

    fn output(&self) -> DataValue {
        let mut source = source::default();
        // dbg!(self.max);
        // dbg!(self.sum);
        let lap = Laplace::new(0.0, self.max / self.epsilon);
        let noise = lap.sample(&mut source);
        DataValue::Float64(self.sum as f64 + noise)
    }
}
