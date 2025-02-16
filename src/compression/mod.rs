use temp_dir::TempDir;

use crate::error::BtrBlocksError;
use crate::ffi::ffi;
use crate::Result;

pub fn compress_column_i32(btr_path: String, data: &Vec<i32>, column_index: u32) -> Result<u32> {
    ffi::compress_column_i32(btr_path, data, column_index)
        .map_err(|err| BtrBlocksError::BtrBlocksLibWrapper(err.to_string()))
}

pub fn compress_column_f64(btr_path: String, data: &Vec<f64>, column_index: u32) -> Result<u32> {
    ffi::compress_column_f64(btr_path, data, column_index)
        .map_err(|err| BtrBlocksError::BtrBlocksLibWrapper(err.to_string()))
}

pub fn compress_column_string(
    btr_path: String,
    data: &Vec<String>,
    column_index: u32,
) -> Result<u32> {
    let binary_path = TempDir::new().map_err(|err| {
        BtrBlocksError::Custom(
            format!("failed to create a temp dir for binary data: {}", err).to_string(),
        )
    })?;
    let binary_path_str = format!(
        "{}/",
        binary_path
            .path()
            .to_str()
            .ok_or(BtrBlocksError::Path("must be a valid path".to_string()))?
    );

    ffi::compress_column_string(btr_path, data, column_index, binary_path_str)
        .map_err(|err| BtrBlocksError::BtrBlocksLibWrapper(err.to_string()))
}
