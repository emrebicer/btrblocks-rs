use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use btrblocks_rs::util::string_to_btr_url;
use btrblocks_rs::{stream::CsvDecompressionStream, Btr, Schema};
use clap::{Parser, Subcommand};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use fuser::MountOption;
use futures::StreamExt;

#[derive(Parser)]
#[command(
    version,
    about = "btr is a program that makes use of `btrblocks_rs` under to hood to interact with btrblocks compressed files",
    long_about = "btr is a program that makes use of `btrblocks_rs` under to hood to interact with btrblocks compressed files.
It can access to multiple objects stores such as local filesystem, Amazon s3, Google Cloud Storage and HTTP file servers.

For some object stores, you should provide your credentials and store information via environment variables;
 - Amazon s3: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION and AWS_BUCKET_NAME",
    arg_required_else_help = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Compress a CSV file into btr format
    FromCsv {
        /// Path to the CSV file to compress
        #[arg(short, long)]
        csv_path: String,

        /// Output btr compressed file path
        #[arg(short, long)]
        btr_path: String,

        /// Path to the YAML file that describes the schema of the CSV file
        #[arg(short, long)]
        schema_path: String,
    },
    /// Decompress a btr file into CSV format
    ToCsv {
        /// Path to the CSV file to compress
        #[arg(short, long)]
        csv_path: String,

        /// Output btr compressed file path
        #[arg(short, long)]
        btr_path: String,
    },
    /// Mount a new file system with fuse and expose the decompressed csv file there
    MountCsv {
        #[arg(short, long)]
        btr_path: String,

        /// The path to mount the file system, the resulting csv can be found under this path
        #[arg(short, long)]
        mount_point: String,

        /// Use one_shot file system for decompressing whole file at once and keep the decompressed
        /// data in memory for faster access (with the downside of higher memory usage)
        #[arg(short, long, default_value_t = false)]
        one_shot: bool,

        /// Calculate the decompressed file size before mounting the filesystem
        ///
        /// While using the realtime decompression filesystem, decompression is only
        /// performed during a read request, hence the full decompressed file size is unknown
        /// untill decompression is performed on all available bytes. Instead of the real size of
        /// bytes, an estimation of file size will be reported via fuse. Which might introduce some
        /// problems when external programs wants to read last bytes directly (like tail).
        ///
        /// Please note that once a full decompression is performed, the file size will be updated to the
        /// correct size. So even though first `tail` usage will result in a wrong result, the
        /// following requests will work as intended.
        ///
        /// But if you want the correct file size in the first place, you can set this option to
        /// true for a full decompression before mounting just for calculating the correct file size.
        ///
        /// This option will only be used if one_shot is set to false (realtimefs is in use)
        #[arg(short, long, default_value_t = false)]
        precompute_csv_size: bool,

        /// Mount flag to allow all users to access files on this filesystem.
        /// By default access is restricted to the user who mounted it
        #[arg(long, default_value_t = false)]
        allow_other: bool,

        /// Mount flag to allow the root user to access this filesystem,
        /// in addition to the user who mounted it
        #[arg(long, default_value_t = false)]
        allow_root: bool,

        /// Mount flag to automatically unmount when the mounting process exits
        #[arg(long, default_value_t = false)]
        auto_unmount: bool,
    },
    /// Decompress the btr compressed file into csv and print the result to stdout
    PrintCsv {
        #[arg(short, long)]
        btr_path: String,
    },
    /// Run an SQL query on the given btr compressed file
    Query {
        #[arg(short, long)]
        btr_path: String,

        /// SQL query to execute, for example "select * from btr where column_0 = 6"
        #[arg(short, long)]
        sql: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Query { btr_path, sql }) => {
            let ctx = SessionContext::new();

            let custom_table_provider =
                btrblocks_rs::datafusion::BtrBlocksDataSource::new(btr_path.to_string()).await;
            ctx.register_table("btr", Arc::new(custom_table_provider))?;
            let df = ctx.sql(sql.as_str()).await?;
            df.show().await?;
        }
        Some(Commands::FromCsv {
            csv_path,
            btr_path,
            schema_path,
        }) => {
            let yaml_content = fs::read_to_string(schema_path)?;
            let schema: Schema = serde_yaml::from_str(&yaml_content)?;

            let csv_url = string_to_btr_url(&mut csv_path.clone())?;
            Btr::from_csv(csv_url, PathBuf::from_str(btr_path)?, schema, true).await?;
        }
        Some(Commands::ToCsv { csv_path, btr_path }) => {
            let btr = Btr::from_url(btr_path.to_string()).await?;
            btr.write_to_csv(csv_path.to_string()).await?;
        }
        Some(Commands::MountCsv {
            mount_point,
            btr_path,
            one_shot,
            allow_other,
            allow_root,
            auto_unmount,
            precompute_csv_size,
        }) => {
            let mut mount_options = vec![];

            if *allow_other {
                mount_options.push(MountOption::AllowOther);
            }

            if *allow_root {
                mount_options.push(MountOption::AllowRoot);
            }

            if *auto_unmount {
                mount_options.push(MountOption::AutoUnmount);
            }

            let btr = Btr::from_url(btr_path.to_string()).await?;

            let _res = if *one_shot {
                btr.mount_csv_one_shot(mount_point.to_string(), &mut mount_options)
                    .await?
            } else {
                btr.mount_csv_realtime(
                    mount_point.to_string(),
                    &mut mount_options,
                    4_000_000,
                    *precompute_csv_size,
                )
                .await?
            };

            // Don't kill the program to keep the file system mounted
            // unless forcefully killed
            loop {
                sleep(Duration::from_secs(1));
            }
        }
        Some(Commands::PrintCsv { btr_path }) => {
            let btr = Btr::from_url(btr_path.to_string()).await?;

            let mut csv_decompression_stream = CsvDecompressionStream::new(btr, 100_000).await?;

            while let Some(batch_res) = csv_decompression_stream.next().await {
                let batch = batch_res?;
                print!("{batch}");
            }
        }
        None => {}
    }

    Ok(())
}
