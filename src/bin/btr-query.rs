use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use clap::Parser;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

/// Simple program to make SQL queries against a BtrBlocks compressed file
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Local path to the BtrBlock compressed file
    #[arg(short, long)]
    btr_path: String,

    /// SQL query to execute, for example "select * from btr where column_0 = 6"
    #[arg(short, long)]
    sql: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let ctx = SessionContext::new();

    let custom_table_provider =
        btrblocks_rs::datafusion::BtrBlocksDataSource::new(PathBuf::from_str(args.btr_path.as_str()).unwrap());
    ctx.register_table("btr", Arc::new(custom_table_provider))
        .unwrap();
    let df = ctx.sql(args.sql.as_str()).await?;
    df.show().await?;
    Ok(())
}
