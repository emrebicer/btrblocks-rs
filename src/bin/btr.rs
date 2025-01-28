use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use btrblocks_rs::{Btr, Schema};
use clap::{Parser, Subcommand};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

#[derive(Parser)]
#[command(version, about, long_about = None, arg_required_else_help = true)]
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
    /// Run an SQL query on the given btr compressed file
    Query {
        #[arg(short, long)]
        btr_path: String,

        /// SQL query to execute, for example "select * from btr where column_0 = 6"
        #[arg(short, long)]
        sql: String,
    },
    /// Mount a new file system with fuse and put the decompressed csv file there
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
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Query { btr_path, sql }) => {
            let ctx = SessionContext::new();

            let custom_table_provider =
                btrblocks_rs::datafusion::BtrBlocksDataSource::new(btr_path.to_string());
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

            Btr::from_csv(
                PathBuf::from_str(csv_path)?,
                PathBuf::from_str(btr_path)?,
                schema,
            )?;
        }
        Some(Commands::ToCsv { csv_path, btr_path }) => {
            let btr = Btr::from_url(btr_path.to_string())?;
            btr.write_to_csv(csv_path.to_string()).await?;
        }
        Some(Commands::MountCsv {
            mount_point,
            btr_path,
            one_shot,
        }) => {
            let btr = Btr::from_url(btr_path.to_string())?;

            let _res = if *one_shot {
                btr.mount_csv_one_shot(mount_point.to_string(), &mut vec![])
                    .await?
            } else {
                btr.mount_csv_realtime(mount_point.to_string(), &mut vec![])
                    .await?
            };

            // Don't kill the program to keep the file system mounted
            // unless forcefully killed
            loop {
                sleep(Duration::from_secs(1));
            }
        }
        None => {}
    }

    Ok(())
}
