use super::*;
use crate::{
    binder::{BoundCopy, FileFormat},
    parser::CopyTarget,
    types::DataType,
};
use std::path::PathBuf;

/// The logical plan of `copy`.
#[derive(Debug, Clone)]
pub struct LogicalCopyFromFile {
    /// The file path to copy from.
    pub path: PathBuf,
    /// The file format.
    pub format: FileFormat,
    /// The column types.
    pub column_types: Vec<DataType>,
}

impl_logical_plan!(LogicalCopyFromFile);

/// The logical plan of `COPY TO`.
#[derive(Debug, Clone)]
pub struct LogicalCopyToFile {
    /// The file path to copy to.
    pub path: PathBuf,
    /// The file format.
    pub format: FileFormat,
    /// The column types.
    pub column_types: Vec<DataType>,
    /// The child plan.
    pub child: LogicalPlanRef,
}

impl_logical_plan!(LogicalCopyToFile, [child]);

impl LogicalPlaner {
    pub fn plan_copy(&self, stmt: BoundCopy) -> Result<LogicalPlanRef, LogicalPlanError> {
        let path = match stmt.target {
            CopyTarget::File { filename } => PathBuf::from(filename),
            t => todo!("unsupported copy target: {:?}", t),
        };
        let column_ids = stmt.columns.iter().map(|col| col.id()).collect();
        let column_types = stmt.columns.iter().map(|col| col.datatype()).collect();
        if stmt.to {
            Ok(LogicalCopyToFile {
                path,
                format: stmt.format,
                column_types,
                child: LogicalGet {
                    table_ref_id: stmt.table_ref_id,
                    column_ids,
                    with_row_handler: false,
                    is_sorted: false,
                }
                .into(),
            }
            .into())
        } else {
            Ok(LogicalInsert {
                table_ref_id: stmt.table_ref_id,
                column_ids,
                child: LogicalCopyFromFile {
                    path,
                    format: stmt.format,
                    column_types,
                }
                .into(),
            }
            .into())
        }
    }
}
