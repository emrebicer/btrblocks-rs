use fuser::{
    BackgroundSession, FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEntry, Request,
};
use libc::ENOENT;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::runtime::Runtime;

use crate::error::BtrBlocksError;
use crate::stream::CsvDecompressionStream;
use crate::{Btr, Result};
use futures::StreamExt;

const TTL: Duration = Duration::from_secs(1);

/// This file system implementation will decompress the btr file per requested bytes
/// instead of holding whole decompressed data in memory, the requested range of bytes
/// will be returned by decompressing the btr file in realtime. The memory consumption will
/// be lower but processing time might be higher per read requests
pub struct BtrBlocksRealtimeFs {
    btr: Btr,
    decompressed_csv_size: usize,
    mount_time: SystemTime,
    cache_limit: usize,
    stream: CsvDecompressionStream,
    raw_csv_data: String,
    decompressed_bytes_count: usize,
    removed_bytes_count: usize,
    ino_to_file: HashMap<u64, (FileType, String)>,
    rt: Arc<Runtime>,
}

impl BtrBlocksRealtimeFs {
    pub async fn new(btr: Btr, decompressed_csv_size: usize, cache_limit: usize) -> Result<Self> {
        Ok(Self {
            btr: btr.clone(),
            decompressed_csv_size,
            mount_time: SystemTime::now(),
            cache_limit,
            stream: CsvDecompressionStream::new(btr, 10_000).await?,
            raw_csv_data: "".to_string(),
            decompressed_bytes_count: 0,
            removed_bytes_count: 0,
            ino_to_file: HashMap::from([
                (1, (FileType::Directory, ".".to_string())),
                (1, (FileType::Directory, "..".to_string())),
                (2, (FileType::RegularFile, "data.csv".to_string())),
            ]),
            rt: Arc::new(Runtime::new().map_err(|e| BtrBlocksError::Custom(e.to_string()))?),
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
        // possibilities,
        // 1- cache is behind requested range (do nothing keep polling / or clear the cache?)
        // 2- start is in cache but end is not (keep polling)
        // 3- both start and end is in cache (perfect just return bytes)
        // 4- cache is past start (just reinit stream from start)

        // Case 1
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
            self.stream = CsvDecompressionStream::new(self.btr.clone(), 10_000).await?;

            self.raw_csv_data.clear();
            self.raw_csv_data.shrink_to_fit();
            self.decompressed_bytes_count = self.raw_csv_data.len();
            self.removed_bytes_count = 0;
        }

        // Keep polling the next rows to find the requested bytes in range
        while let Some(batch) = self.stream.next().await {
            let batch = batch.map_err(|err| BtrBlocksError::Custom(err.to_string()))?;

            self.raw_csv_data.push_str(&batch);
            self.decompressed_bytes_count += batch.len();

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
            // We never should en up here... Because both start and end is
            // in the decompressed area, which should have been returned earlier anyways
            Err(BtrBlocksError::Custom(
                "Undefined behaviour, failed to decompress requested bytes".to_string(),
            ))
        }
    }

    fn clear_cache(&mut self) {
        if self.raw_csv_data.len() > self.cache_limit {
            let bytes_to_remove = self.raw_csv_data.len() - self.cache_limit;
            self.raw_csv_data.drain(..bytes_to_remove);
            self.raw_csv_data.shrink_to_fit();
            self.removed_bytes_count += bytes_to_remove;
        }
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
            let end = (start + size as usize).min(self.decompressed_csv_size);

            // Ensure the offset is within bounds
            if start >= self.decompressed_csv_size {
                reply.data(&[]);
                return;
            }

            let res = self
                .rt
                .clone()
                .block_on(self.decompress_from_range(start, end))
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
