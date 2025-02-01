use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::SchemaRef;
use fuser::{
    BackgroundSession, FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEntry, Request,
};
use libc::ENOENT;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::time::{Duration, SystemTime};
use tokio::runtime::{Handle, Runtime};
use tokio::task::block_in_place;

use crate::datafusion::BtrChunkedStream;
use crate::error::BtrBlocksError;
use crate::util::extract_value_as_string;
use crate::{Btr, Result};
use futures::StreamExt;

const TTL: Duration = Duration::from_secs(1);

/// This file system implementation will decompress the btr file per requested bytes
/// instead of holding whole decompressed data in memory, the requested range of bytes
/// will be returned by decompressing the btr file in realtime. The memory consumption will
/// be lower but processing time might be higher per read requests
pub struct BtrBlocksRealtimeFs {
    btr: Btr,
    schema_ref: SchemaRef,
    decompressed_csv_size: usize,
    mount_time: SystemTime,
    csv_header: String,
    column_count: usize,
    cache_limit: usize,
    stream: BtrChunkedStream,
    raw_csv_data: String,
    decompressed_bytes_count: usize,
    removed_bytes_count: usize,
    ino_to_file: HashMap<u64, (FileType, String)>,
}

impl BtrBlocksRealtimeFs {
    pub async fn new(
        btr: Btr,
        decompressed_csv_size: usize,
        schema_ref: SchemaRef,
        csv_header: String,
        column_count: usize,
        cache_limit: usize,
    ) -> Result<Self> {
        Ok(Self {
            btr: btr.clone(),
            decompressed_csv_size,
            schema_ref: schema_ref.clone(),
            mount_time: SystemTime::now(),
            csv_header: csv_header.clone(),
            column_count,
            cache_limit,
            stream: BtrChunkedStream::new(schema_ref, btr, 10_000).await?,
            raw_csv_data: csv_header.clone(),
            decompressed_bytes_count: csv_header.len(),
            removed_bytes_count: 0,
            ino_to_file: HashMap::from([
                (1, (FileType::Directory, ".".to_string())),
                (1, (FileType::Directory, "..".to_string())),
                (2, (FileType::RegularFile, "data.csv".to_string())),
            ]),
        })
    }

    fn csv_file_attr(&self) -> FileAttr {
        FileAttr {
            ino: 2,
            size: self.decompressed_csv_size as u64,
            blocks: (self.decompressed_csv_size as u64 + 511) / 512,
            atime: self.mount_time,
            mtime: self.mount_time,
            ctime: self.mount_time,
            crtime: self.mount_time,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            flags: 0,
            //flags: libc::O_DIRECT as u32, // hint direct_io to disable kernel caching
            blksize: 4096,
        }
    }

    pub fn mount(
        self,
        mount_point: String,
        mount_options: &mut Vec<MountOption>,
    ) -> Result<BackgroundSession> {
        let mut options = vec![
            MountOption::RO,
            MountOption::NoExec,
            MountOption::FSName("BtrBlocksRealtimeFs".to_string()),
        ];
        options.append(mount_options);
        fuser::spawn_mount2(self, mount_point, &options)
            .map_err(|err| BtrBlocksError::Mount(err.to_string()))
    }

    async fn decompress_from_range(&mut self, start: usize, end: usize) -> Result<String> {
        //println!("new read request, start: {start}, end: {end}");

        // possibilities,
        // 1- cache is behind requested range (do nothing keep polling / or clear the cache?)
        // 2- start is in cache but end is not (keep polling)
        // 3- both start and end is in cache (perfect just return bytes)
        // 4- cache is past start (just reinit stream from start)

        if self.decompressed_bytes_count < start {
            self.clear_cache();
        }

        // Check if the requested range of bytes are already in the cache (perfect case 3)
        if start >= self.removed_bytes_count && end <= self.decompressed_bytes_count {
            //println!("@@@ CACHE HIT! Just returning result...");
            let start_index = self.decompressed_bytes_count
                - self.removed_bytes_count
                - (self.decompressed_bytes_count - start);
            let end_index = start_index + (end - start);
            //println!("start_index: {start_index}, end_index: {end_index}");
            let res = self.raw_csv_data[start_index..end_index].to_string();
            self.clear_cache();
            return Ok(res);
        }

        // Check if cache is past end (case 4, reinit stream)
        if self.removed_bytes_count > start {
            //println!("@@@@ CACHE MISS! Reinit stream...");
            self.stream =
                BtrChunkedStream::new(self.schema_ref.clone(), self.btr.clone(), 10_000).await?;

            //self.raw_csv_data = String::new();
            self.raw_csv_data.clear();
            self.raw_csv_data.shrink_to_fit();

            // Write the header row to the target
            self.raw_csv_data.push_str(self.csv_header.as_str());
            self.decompressed_bytes_count = self.raw_csv_data.len();
            self.removed_bytes_count = 0;
        }

        // Keep polling the next rows to find the requested bytes in range
        while let Some(batch) = self.stream.next().await {
            let batch = batch.map_err(|err| BtrBlocksError::Custom(err.to_string()))?;
            let num_rows = batch.num_rows();
            let num_columns = batch.num_columns();

            let columns: Vec<&dyn Array> = (0..num_columns)
                .map(|col_index| batch.column(col_index).as_ref())
                .collect();

            for row_index in 0..num_rows {
                self.raw_csv_data.push('\n');
                self.decompressed_bytes_count += 1;

                for (counter, column) in columns.iter().enumerate() {
                    let value = extract_value_as_string(*column, row_index);
                    self.raw_csv_data.push_str(&value.to_string());
                    self.decompressed_bytes_count += value.len();

                    if counter + 1 < self.column_count {
                        self.raw_csv_data.push(',');
                        self.decompressed_bytes_count += 1;
                    }
                }
            }

            // Check if our current decompressed data has reached the start
            // There are 3 possibilities
            //   - decompressed size has not reached the start
            //      - Clear the cache
            //   - decompressed size has reached the start but not reached the end
            //      - Don't clear the cache keep decompressing
            //   - decompressed size has reached the start and reached the end
            //      - Return the requested chunk
            //
            //println!("Polled stream, decompressed_bytes_count: {decompressed_bytes_count}, removed_bytes_count: {removed_bytes_count}");
            if self.decompressed_bytes_count < start {
                self.clear_cache();
            } else if self.decompressed_bytes_count >= start && self.decompressed_bytes_count >= end
            {
                let start_index = self.decompressed_bytes_count
                    - self.removed_bytes_count
                    - (self.decompressed_bytes_count - start);
                let end_index = start_index + (end - start);
                let res = self.raw_csv_data[start_index..end_index].to_string();
                self.clear_cache();
                return Ok(res);
            }
        }

        // We have reached the end of the decompressed data
        // If we have reached here, we now  calculate the actual decompressed full size
        // so update it
        //println!(
        //    "No more files to decompress... Old size: {}, new size: {}",
        //    self.decompressed_csv_size, self.decompressed_bytes_count
        //);
        self.decompressed_csv_size = self.decompressed_bytes_count;

        if start > self.decompressed_csv_size {
            // A non existent area is requested just return empty
            Ok("".to_string())
        } else if start <= self.decompressed_csv_size && end > self.decompressed_csv_size {
            // Some part of the requested area exists, just return the existing bytes
            let start_index = self.decompressed_bytes_count
                - self.removed_bytes_count
                - (self.decompressed_bytes_count - start);
            Ok(self.raw_csv_data[start_index..].to_string())
        } else {
            // we never should en up here... Because both start and end is
            // in the decompressed area, which shoul have been returned earlier anyways
            Err(BtrBlocksError::Custom(
                "Undefined behaviour, failed to decompress requested bytes".to_string(),
            ))
        }

        //// Approach 2, just pad the rest of the bytes with new line
        //if start > self.decompressed_bytes_count {
        //    //return Ok("\0".repeat(end - start).to_string());
        //    return Ok("".to_string());
        //} else {
        //    // We have some bytes that we want to have from the cache
        //    // so pad the cache with \n until we have enough bytes
        //    //let missing_len = end - self.decompressed_bytes_count;
        //    //self.raw_csv_data.push_str(&"\0".repeat(missing_len));
        //    //self.decompressed_bytes_count += missing_len;
        //    //return Ok(self.raw_csv_data.clone());
        //
        //    //let missing_len = end - self.decompressed_bytes_count;
        //    //self.raw_csv_data.push_str(&"\0".repeat(missing_len));
        //    //self.decompressed_bytes_count += missing_len;
        //    return Ok(self.raw_csv_data.clone());
        //}

        //Err(BtrBlocksError::Custom(
        //    "Failed to decompress from range".to_string(),
        //))
    }

    fn clear_cache(&mut self) {
        if self.raw_csv_data.len() > self.cache_limit {
            let bytes_to_remove = self.raw_csv_data.len() - self.cache_limit;
            //println!(
            //    "@@ Clearing unused cache... current cache size: {}, bytes to remove: {}",
            //    self.raw_csv_data.len(),
            //    bytes_to_remove
            //);
            self.raw_csv_data.drain(..bytes_to_remove);
            self.raw_csv_data.shrink_to_fit();
            //println!("1st case, clearing");
            self.removed_bytes_count += bytes_to_remove;
        }
        //else {
        //    println!("Not clearing cache, the size is only: {}", self.raw_csv_data.len());
        //}
    }
}

impl Filesystem for BtrBlocksRealtimeFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent == 1 && name.to_str() == Some("data.csv") {
            reply.entry(&TTL, &self.csv_file_attr(), 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match ino {
            // Parent directory
            1 => reply.attr(
                &TTL,
                &FileAttr {
                    ino: 1,
                    size: 0,
                    blocks: 0,
                    atime: self.mount_time,
                    mtime: self.mount_time,
                    ctime: self.mount_time,
                    crtime: self.mount_time,
                    kind: FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: unsafe { libc::getuid() },
                    gid: unsafe { libc::getgid() },
                    rdev: 0,
                    flags: 0,
                    blksize: 4096,
                },
            ),
            // The CSV file
            2 => reply.attr(&TTL, &self.csv_file_attr()),
            // No other files are present
            _ => reply.error(ENOENT),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        if ino == 2 {
            // The requested bytes are whole_data[offset..end]
            let start = offset as usize;

            // Calculate the end of the slice
            let end = (start + size as usize).min(self.decompressed_csv_size as usize);

            // Ensure the offset is within bounds
            if start >= self.decompressed_csv_size as usize {
                reply.data(&[]);
                return;
            }

            let runtime = Runtime::new().expect("should create a new tokio runtime");

            let res = runtime
                .block_on(async {
                    block_in_place(|| {
                        let value = tokio::runtime::Handle::current()
                            .block_on(async { self.decompress_from_range(start, end).await });
                        value
                    })
                })
                .expect("failed to decompress from range");

            reply.data(res.as_bytes());
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != 1 {
            reply.error(ENOENT);
            return;
        }

        for (i, entry) in self
            .ino_to_file
            .clone()
            .into_iter()
            .enumerate()
            .skip(offset as usize)
        {
            // i + 1 means the index of the next entry
            if reply.add(entry.0, (i + 1) as i64, entry.1 .0, entry.1 .1) {
                break;
            }
        }
        reply.ok();
    }
}
