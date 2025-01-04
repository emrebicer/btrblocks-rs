// TODO: add tests for column decompressions to file
// TODO: add  tests for datafusion sql query impl
mod btrblocks;
mod error;
pub mod datafusion;
mod ffi;

pub use btrblocks::*;

pub type Result<T> = std::result::Result<T, error::BtrBlocksError>;

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::Mutex;

    use rand::rngs::StdRng;
    use rand::Rng;
    use rand::SeedableRng;
    use temp_dir::TempDir;

    use crate::ColumnType;

    // Prevent test running in parallel,
    // btrblocks does not seem to work well with parallel execution
    static TEST_EXECUTER: Mutex<Executor> = Mutex::new(Executor);
    #[derive(Clone, Copy)]
    struct Executor;

    impl Executor {
        fn run_test(self, f: impl FnOnce()) {
            f();
        }
    }

    fn get_mock_ids() -> Vec<i32> {
        vec![1, 2, 3]
    }

    fn get_mock_names() -> Vec<String> {
        vec!["Julia".to_string(), "Peter".to_string(), "Jack".to_string()]
    }

    fn get_mock_scores() -> Vec<f64> {
        vec![0.123, 213.1232, 4.20]
    }

    fn create_temp_btr_from_csv(temp_files_dir: &TempDir, temp_btr_dir: &TempDir) -> crate::Btr {
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

        let res = crate::Btr::from_csv(csv_path, btr_path, schema);
        assert!(res.is_ok());

        res.unwrap()
    }

    #[test]
    fn csv_to_btr() {
        TEST_EXECUTER.lock().unwrap().run_test(|| {
            let temp_files_dir =
                TempDir::new().expect("should not fail to create a temp dir for csv data");
            let btr_temp_dir = TempDir::new().unwrap();
            let _ = create_temp_btr_from_csv(&temp_files_dir, &btr_temp_dir);
        });
    }

    #[test]
    fn btr_decompress_by_parts() {
        // TODO: this test is not that meaningful as there will be only 1 part per column
        // with this little data... I can generate way more random data on the fly but this
        // will hinder the UX as running tests will be way slower, ideally find a way to 
        // force creating multiple parts with little data, the configure methods does not seem to
        // work for this...
        TEST_EXECUTER.lock().unwrap().run_test(|| {
            let temp_files_dir =
                TempDir::new().expect("should not fail to create a temp dir for csv data");
            let btr_temp_dir = TempDir::new().unwrap();
            let btr = create_temp_btr_from_csv(&temp_files_dir, &btr_temp_dir);

            let meta = btr.file_metadata().unwrap();

            for (col_index, part_info) in meta.parts.iter().enumerate() {
                match part_info.r#type {
                    ColumnType::Integer => {
                        // Read all data by iterating over parts
                        let mut parts_vec = Vec::new();
                        println!("number of int parts: {}", part_info.num_parts);
                        for part_index in 0..part_info.num_parts {
                            parts_vec.append(
                                &mut btr
                                    .decompress_column_part_i32(col_index as u32, part_index)
                                    .expect("decompression should not fail"),
                            );
                        }

                        // Read all data at once
                        let column_vec = btr
                            .decompress_column_i32(col_index as u32)
                            .expect("decompression should not fail");

                        assert_eq!(parts_vec, column_vec);
                    }
                    ColumnType::Double => {
                        // Read all data by iterating over parts
                        let mut parts_vec = Vec::new();
                        println!("number of f64 parts: {}", part_info.num_parts);
                        for part_index in 0..part_info.num_parts {
                            parts_vec.append(
                                &mut btr
                                    .decompress_column_part_f64(col_index as u32, part_index)
                                    .expect("decompression should not fail"),
                            );
                        }

                        // Read all data at once
                        let column_vec = btr
                            .decompress_column_f64(col_index as u32)
                            .expect("decompression should not fail");

                        assert_eq!(parts_vec, column_vec);
                    }
                    ColumnType::String => {
                        // Read all data by iterating over parts
                        let mut parts_vec = Vec::new();
                        println!("number of string parts: {}", part_info.num_parts);
                        for part_index in 0..part_info.num_parts {
                            parts_vec.append(
                                &mut btr
                                    .decompress_column_part_string(col_index as u32, part_index)
                                    .expect("decompression should not fail"),
                            );
                        }

                        // Read all data at once
                        let column_vec = btr
                            .decompress_column_string(col_index as u32)
                            .expect("decompression should not fail");

                        assert_eq!(parts_vec, column_vec);
                    }
                    _ => {}
                }
            }
        });
    }

    #[test]
    fn btr_decompress_column_i32() {
        TEST_EXECUTER.lock().unwrap().run_test(|| {
            let temp_files_dir =
                TempDir::new().expect("should not fail to create a temp dir for csv data");
            let temp_btr_dir = TempDir::new().unwrap();
            let btr = create_temp_btr_from_csv(&temp_files_dir, &temp_btr_dir);
            let ids = btr.decompress_column_i32(0).unwrap();
            assert_eq!(ids, get_mock_ids());
        });
    }

    #[test]
    fn btr_decompress_column_string() {
        TEST_EXECUTER.lock().unwrap().run_test(|| {
            let temp_files_dir =
                TempDir::new().expect("should not fail to create a temp dir for csv data");
            let temp_btr_dir = TempDir::new().unwrap();
            let btr = create_temp_btr_from_csv(&temp_files_dir, &temp_btr_dir);
            let names = btr.decompress_column_string(1).unwrap();
            assert_eq!(names, get_mock_names());
        });
    }

    #[test]
    fn btr_decompress_column_double() {
        TEST_EXECUTER.lock().unwrap().run_test(|| {
            let temp_files_dir =
                TempDir::new().expect("should not fail to create a temp dir for csv data");
            let temp_btr_dir = TempDir::new().unwrap();
            let btr = create_temp_btr_from_csv(&temp_files_dir, &temp_btr_dir);
            let scores = btr.decompress_column_f64(2).unwrap();
            assert_eq!(scores, get_mock_scores());
        });
    }

    #[test]
    fn random_int_double_compression() {
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
