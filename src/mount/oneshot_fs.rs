use fuser::{
    BackgroundSession, FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEntry, Request,
};
use libc::ENOENT;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};

use crate::error::BtrBlocksError;
use crate::Result;

const TTL: Duration = Duration::from_secs(1);

/// This file system implementation will decompress the btr file into a csv file and make the whole
/// decompressed data available on the mounted fs under data.csv file, it will be faster to access
/// the file once it is decompressed but since the decompressed contents will be stored in memory
/// the memory consumption will be high for large files
pub struct BtrBlocksOneShotFs {
    raw_csv_data: String,
    ino_to_file: HashMap<u64, (FileType, String)>,
}

impl BtrBlocksOneShotFs {
    pub fn new(raw_csv_data: String) -> Self {
        Self {
            raw_csv_data,
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
            size: self.raw_csv_data.as_bytes().len() as u64,
            blocks: ((self.raw_csv_data.as_bytes().len() + 511) / 512) as u64,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: 501,
            gid: 20,
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
            MountOption::FSName("BtrBlocksOneShotFs".to_string()),
        ];
        options.append(mount_options);
        fuser::spawn_mount2(self, mount_point, &options)
            .map_err(|err| BtrBlocksError::Mount(err.to_string()))
    }
}

impl Filesystem for BtrBlocksOneShotFs {
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
                    atime: UNIX_EPOCH,
                    mtime: UNIX_EPOCH,
                    ctime: UNIX_EPOCH,
                    crtime: UNIX_EPOCH,
                    kind: FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: 501,
                    gid: 20,
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
            let data = self.raw_csv_data.as_bytes();
            let offset = offset as usize;

            // Ensure the offset is within bounds
            if offset >= data.len() {
                reply.data(&[]);
                return;
            }

            // Calculate the end of the slice
            let end = (offset + size as usize).min(data.len());
            reply.data(&data[offset..end]);
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
