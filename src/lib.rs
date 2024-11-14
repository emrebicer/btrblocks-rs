mod btrblocks;
mod ffi;

pub use btrblocks::*;

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::Rng;
    use rand::SeedableRng;

    #[test]
    fn random_int_double_compression() {
        crate::configure(3);
        crate::set_log_level(crate::LogLevel::Info);

        let relation = crate::Relation::new();

        let int_vec = crate::IntMMapVector::new(&generate_data(640000, (1 << 12) - 1, 40, 69));
        relation.add_column_int(&"ints".to_string(), int_vec);

        let double_vec =
            crate::DoubleMMapVector::new(&generate_data(640000, (1 << 12) - 1, 40, 42));
        relation.add_column_double(&"dbls".to_string(), double_vec);

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
