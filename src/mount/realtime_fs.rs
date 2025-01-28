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
use tokio::runtime::Runtime;
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
    decompressed_csv_size: u64,
    mount_time: SystemTime,
    csv_header: String,
    column_count: usize,
    ino_to_file: HashMap<u64, (FileType, String)>,
}

impl BtrBlocksRealtimeFs {
    pub fn new(
        btr: Btr,
        decompressed_csv_size: u64,
        schema_ref: SchemaRef,
        csv_header: String,
        column_count: usize,
    ) -> Self {
        Self {
            btr,
            decompressed_csv_size,
            schema_ref,
            mount_time: SystemTime::now(),
            csv_header,
            column_count,
            ino_to_file: HashMap::from([
                (1, (FileType::Directory, ".".to_string())),
                (1, (FileType::Directory, "..".to_string())),
                (2, (FileType::RegularFile, "data.csv".to_string())),
            ]),
        }
    }

    fn csv_file_attr(&self) -> FileAttr {
        FileAttr {
            ino: 2,
            size: self.decompressed_csv_size,
            blocks: (self.decompressed_csv_size + 511) / 512,
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

    // TODO: compare ram usage with other mount implementation (just check head and tail)
    // cache the latest poll result, this will be a low overhead on ram but huge gain
    // on cpu time as we would not need to start decompressing over and over again...
    async fn decompress_from_range(&self, start: usize, end: usize) -> Result<String> {
        //println!("new read request, start: {start}, end: {end}");
        let mut stream =
            BtrChunkedStream::new(self.schema_ref.clone(), self.btr.clone(), 100_000).await?;

        // data to cache within FS: stream, raw_csv_data, decompressed_bytes_count, removed_bytes_count

        let mut raw_csv_data = String::new();

        // Write the header row to the target
        raw_csv_data.push_str(self.csv_header.as_str());
        let mut decompressed_bytes_count = raw_csv_data.len();
        let mut removed_bytes_count = 0;

        // Write the rows data in batches
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|err| BtrBlocksError::Custom(err.to_string()))?;
            let num_rows = batch.num_rows();
            let num_columns = batch.num_columns();

            let columns: Vec<&dyn Array> = (0..num_columns)
                .map(|col_index| batch.column(col_index).as_ref())
                .collect();

            for row_index in 0..num_rows {
                raw_csv_data.push('\n');
                decompressed_bytes_count += 1;

                for (counter, column) in columns.iter().enumerate() {
                    let value = extract_value_as_string(*column, row_index);
                    raw_csv_data.push_str(&value.to_string());
                    decompressed_bytes_count += value.len();

                    if counter + 1 < self.column_count {
                        raw_csv_data.push(',');
                        decompressed_bytes_count += 1;
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
            if decompressed_bytes_count < start {
                //println!("1st case, clearing");
                removed_bytes_count += raw_csv_data.len();
                raw_csv_data.clear();
            } else if decompressed_bytes_count >= start && decompressed_bytes_count >= end {
                //println!("2nd case...");
                //println!("start: {start}, end: {end}");
                let start_index = decompressed_bytes_count
                    - removed_bytes_count
                    - (decompressed_bytes_count - start);
                let end_index = start_index + (end - start);
                //println!("start_index: {start_index}, end_index: {end_index}");
                let a = raw_csv_data[start_index..end_index].to_string();
                //println!("a is: {a}");
                return Ok(a);
            }
        }
        Err(BtrBlocksError::Custom(
            "Failed to decompress from range".to_string(),
        ))
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

            let runtime = Runtime::new().unwrap();

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
