mod btrblocks;
pub mod datafusion;
mod error;
mod ffi;
mod mount;
mod util;

pub use btrblocks::*;

pub type Result<T> = std::result::Result<T, error::BtrBlocksError>;

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::Arc;

    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::array::StringArray;
    use datafusion::prelude::SessionContext;
    use rand::rngs::StdRng;
    use rand::Rng;
    use rand::SeedableRng;
    use serial_test::serial;
    use temp_dir::TempDir;

    use crate::datafusion::BtrBlocksDataSource;
    use crate::ColumnType;

    fn get_mock_ids() -> Vec<i32> {
        vec![1, 2, 3]
    }

    fn get_mock_names() -> Vec<String> {
        vec!["Julia".to_string(), "Peter".to_string(), "Jack".to_string()]
    }

    fn get_mock_scores() -> Vec<f64> {
        vec![0.123, 213.1232, 4.20]
    }

    async fn create_temp_btr_from_csv(temp_files_dir: &TempDir, temp_btr_dir: &TempDir) -> crate::Btr {
        // Create temp csv file
        let csv_path = PathBuf::from_str(
            format!("{}/data.csv", temp_files_dir.path().to_str().unwrap()).as_str(),
        )
        .unwrap();

        let mut csv_file = File::create(csv_path.clone()).unwrap();
        let ids = get_mock_ids();
        let names = get_mock_names();
        let scores = get_mock_scores();

        assert_eq!(ids.len(), names.len());
        assert_eq!(names.len(), scores.len());

        for i in 0..ids.len() {
            csv_file
                .write(ids.get(i).unwrap().to_string().as_str().as_bytes())
                .unwrap();
            csv_file.write(",".as_bytes()).unwrap();
            csv_file
                .write(names.get(i).unwrap().as_str().as_bytes())
                .unwrap();
            csv_file.write(",".as_bytes()).unwrap();
            csv_file
                .write(scores.get(i).unwrap().to_string().as_str().as_bytes())
                .unwrap();
            csv_file.write("\n".as_bytes()).unwrap();
        }

        let btr_path = PathBuf::from_str(temp_btr_dir.path().to_str().unwrap()).unwrap();

        let schema = crate::Schema::new(vec![
            crate::ColumnMetadata::new("Id".to_string(), crate::ColumnType::Integer),
            crate::ColumnMetadata::new("Name".to_string(), crate::ColumnType::String),
            crate::ColumnMetadata::new("Score".to_string(), crate::ColumnType::Double),
        ]);

        let res = crate::Btr::from_csv(csv_path, btr_path, schema).await;
        assert!(res.is_ok());

        res.unwrap()
    }

    #[tokio::test]
    #[serial]
    async fn csv_to_btr() {
        let temp_files_dir =
            TempDir::new().expect("should not fail to create a temp dir for csv data");
        let btr_temp_dir = TempDir::new().unwrap();
        let _ = create_temp_btr_from_csv(&temp_files_dir, &btr_temp_dir);
    }

    #[tokio::test]
    #[serial]
    async fn btr_to_csv() {
        let temp_files_dir =
            TempDir::new().expect("should not fail to create a temp dir for csv data");
        let btr_temp_dir = TempDir::new().unwrap();
        let btr = create_temp_btr_from_csv(&temp_files_dir, &btr_temp_dir).await;

        let temp_csv_dir =
            TempDir::new().expect("should not fail to create a temp dir for csv data");

        let temp_csv_path = format!(
            "{}/result.csv",
            temp_csv_dir.path().to_str().unwrap().to_string()
        );

        let res = btr.write_to_csv(temp_csv_path.clone()).await;

        assert!(res.is_ok());

        // Check if the contents of the decompressed csv file is correct
        let mut correct_csv_data = String::from("column_0,column_1,column_2\n");

        let ids = get_mock_ids();
        let names = get_mock_names();
        let scores = get_mock_scores();

        for i in 0..ids.len() {
            correct_csv_data.push_str(ids.get(i).unwrap().to_string().as_str());
            correct_csv_data.push(',');

            correct_csv_data.push_str(names.get(i).unwrap().to_string().as_str());
            correct_csv_data.push(',');

            correct_csv_data.push_str(scores.get(i).unwrap().to_string().as_str());
            if i + 1 < ids.len() {
                correct_csv_data.push('\n');
            }
        }

        // Read the decompressed data
        let result_csv_data = fs::read_to_string(temp_csv_path).unwrap();

        assert_eq!(correct_csv_data, result_csv_data);
    }

    #[tokio::test]
    #[serial]
    async fn mount_csv() {
        let temp_files_dir =
            TempDir::new().expect("should not fail to create a temp dir for csv data");
        let btr_temp_dir = TempDir::new().unwrap();
        let btr = create_temp_btr_from_csv(&temp_files_dir, &btr_temp_dir).await;

        let temp_csv_dir =
            TempDir::new().expect("should not fail to create a temp dir for csv data");

        let temp_csv_path = format!(
            "{}/data.csv",
            temp_csv_dir.path().to_str().unwrap().to_string()
        );

        let res = btr
            .mount_csv_one_shot(
                temp_csv_dir.path().to_str().unwrap().to_string(),
                &mut vec![],
            )
            .await;

        assert!(res.is_ok());

        // Check if the contents of the decompressed csv file is correct
        let mut correct_csv_data = String::from("column_0,column_1,column_2\n");

        let ids = get_mock_ids();
        let names = get_mock_names();
        let scores = get_mock_scores();

        for i in 0..ids.len() {
            correct_csv_data.push_str(ids.get(i).unwrap().to_string().as_str());
            correct_csv_data.push(',');

            correct_csv_data.push_str(names.get(i).unwrap().to_string().as_str());
            correct_csv_data.push(',');

            correct_csv_data.push_str(scores.get(i).unwrap().to_string().as_str());
            if i + 1 < ids.len() {
                correct_csv_data.push('\n');
            }
        }

        // Read the decompressed data
        let result_csv_data = fs::read_to_string(temp_csv_path).unwrap();

        assert_eq!(correct_csv_data, result_csv_data);
    }

    #[tokio::test]
    #[serial]
    async fn sql_query() {
        let temp_files_dir =
            TempDir::new().expect("should not fail to create a temp dir for csv data");
        let btr_temp_dir = TempDir::new().unwrap();
        let _btr = create_temp_btr_from_csv(&temp_files_dir, &btr_temp_dir).await;

        let ctx = SessionContext::new();

        let custom_table_provider =
            BtrBlocksDataSource::new(btr_temp_dir.path().to_str().unwrap().to_string()).await;
        ctx.register_table("btr", Arc::new(custom_table_provider))
            .unwrap();

        let sql = "select * from btr where column_0 = 3";

        let df = ctx.sql(sql).await.unwrap();
        let res = df.collect().await.unwrap();

        let id = res
            .get(0)
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);

        assert_eq!(3, id);

        let name = res
            .get(0)
            .unwrap()
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);

        assert_eq!("Jack", name);

        let score = res
            .get(0)
            .unwrap()
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);

        assert_eq!(4.20, score);
    }

    #[tokio::test]
    #[serial]
    async fn btr_decompress_by_parts() {
        // TODO: this test is not that meaningful as there will be only 1 part per column
        // with this little data... I can generate way more random data on the fly but this
        // will hinder the UX as running tests will be way slower, ideally find a way to
        // force creating multiple parts with little data, the configure methods does not seem to
        // work for this...
        let temp_files_dir =
            TempDir::new().expect("should not fail to create a temp dir for csv data");
        let btr_temp_dir = TempDir::new().unwrap();
        let btr = create_temp_btr_from_csv(&temp_files_dir, &btr_temp_dir).await;

        let meta = btr.file_metadata().await.unwrap();

        for (col_index, part_info) in meta.parts.iter().enumerate() {
            match part_info.r#type {
                ColumnType::Integer => {
                    // Read all data by iterating over parts
                    let mut parts_vec = Vec::new();
                    for part_index in 0..part_info.num_parts {
                        let mut res = btr
                            .decompress_column_part_i32(col_index as u32, part_index)
                            .await
                            .expect("decompression should not fail");
                        parts_vec.append(&mut res);
                    }

                    // Read all data at once
                    let column_vec = btr
                        .decompress_column_i32(col_index as u32)
                        .await
                        .expect("decompression should not fail");

                    assert_eq!(parts_vec, column_vec);
                }
                ColumnType::Double => {
                    // Read all data by iterating over parts
                    let mut parts_vec = Vec::new();
                    for part_index in 0..part_info.num_parts {
                        let mut res = btr
                            .decompress_column_part_f64(col_index as u32, part_index)
                            .await
                            .expect("decompression should not fail");
                        parts_vec.append(&mut res);
                    }

                    // Read all data at once
                    let column_vec = btr
                        .decompress_column_f64(col_index as u32)
                        .await
                        .expect("decompression should not fail");

                    assert_eq!(parts_vec, column_vec);
                }
                ColumnType::String => {
                    // Read all data by iterating over parts
                    let mut parts_vec = Vec::new();
                    for part_index in 0..part_info.num_parts {
                        let mut res = btr
                            .decompress_column_part_string(col_index as u32, part_index)
                            .await
                            .expect("decompression should not fail");
                        parts_vec.append(&mut res);
                    }

                    // Read all data at once
                    let column_vec = btr
                        .decompress_column_string(col_index as u32)
                        .await
                        .expect("decompression should not fail");

                    assert_eq!(parts_vec, column_vec);
                }
                _ => {}
            }
        }
    }

    #[tokio::test]
    #[serial]
    async fn btr_decompress_column_i32() {
        let temp_files_dir =
            TempDir::new().expect("should not fail to create a temp dir for csv data");
        let temp_btr_dir = TempDir::new().unwrap();
        let btr = create_temp_btr_from_csv(&temp_files_dir, &temp_btr_dir).await;
        let ids = btr.decompress_column_i32(0).await.unwrap();
        assert_eq!(ids, get_mock_ids());
    }

    #[tokio::test]
    #[serial]
    async fn btr_decompress_column_string() {
        let temp_files_dir =
            TempDir::new().expect("should not fail to create a temp dir for csv data");
        let temp_btr_dir = TempDir::new().unwrap();
        let btr = create_temp_btr_from_csv(&temp_files_dir, &temp_btr_dir).await;
        let names = btr.decompress_column_string(1).await.unwrap();
        assert_eq!(names, get_mock_names());
    }

    #[tokio::test]
    #[serial]
    async fn btr_decompress_column_double() {
        let temp_files_dir =
            TempDir::new().expect("should not fail to create a temp dir for csv data");
        let temp_btr_dir = TempDir::new().unwrap();
        let btr = create_temp_btr_from_csv(&temp_files_dir, &temp_btr_dir).await;
        let scores = btr.decompress_column_f64(2).await.unwrap();
        assert_eq!(scores, get_mock_scores());
    }

    #[tokio::test]
    #[serial]
    async fn random_int_double_compression() {
        crate::configure(3, 65536);
        crate::set_log_level(crate::LogLevel::Info);

        let relation = crate::Relation::new();

        let int_vec = crate::IntMMapVector::new(&generate_data(640000, (1 << 12) - 1, 40, 69));
        relation.add_column_int("ints".to_string(), int_vec);

        let double_vec =
            crate::DoubleMMapVector::new(&generate_data(640000, (1 << 12) - 1, 40, 42));
        relation.add_column_double("dbls".to_string(), double_vec);

        let datablock = crate::Datablock::new(&relation);

        let tuple_count = relation.tuple_count();
        let input = relation.chunk(&vec![(0, tuple_count)], 0);

        let size = input.tuple_count() as usize * std::mem::size_of::<f64>() * 2;

        // Create a buffer for output (it is just a unique pointer to [u8])
        let output = crate::Buffer::new(size);

        // Compress the input
        let stats = datablock.compress(&input, &output);

        println!("Stats:");
        println!("\t Input size: {}", input.size_bytes());
        println!("\t Output size: {}", stats.total_data_size());
        println!("\t Compression ratio: {}", stats.compression_ratio());

        // Decompress the output
        let decompressed = datablock.decompress(&output);

        // Compare the input and output
        let res = relation.compare_chunks(&input, &decompressed);

        if res {
            println!("decompressed data matches original data");
        } else {
            println!("decompressed data does NOT match original data");
        }
    }

    pub fn generate_data<T>(size: usize, unique: usize, runlength: usize, seed: u64) -> Vec<T>
    where
        T: num::FromPrimitive + num::ToPrimitive + Copy,
    {
        let mut data: Vec<T> = Vec::with_capacity(size);
        let mut rng = StdRng::seed_from_u64(seed);

        let mut i = 0;
        while i < size - runlength {
            let number = T::from_usize((rng.gen::<usize>() % unique) as usize).unwrap();
            for _ in 0..runlength {
                data.push(number);
                i += 1;
            }

            i += 1;
        }

        data
    }
}
