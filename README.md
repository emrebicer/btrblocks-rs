# BtrBlocks-rs
BtrBlocks is an Efficient Columnar Compression for Data Lakes (SIGMOD 2023 Paper)
- [Paper](https://bit.ly/btrblocks) (two-column version)
- [Video](https://dl.acm.org/doi/10.1145/3589263) (SIGMOD 2023 presentation)

This library provides an interface to the [BtrBlocks](https://github.com/maxi-k/btrblocks) C++ library with ergonomic Rust types. It does that by creating bindings to the custom C++ wrapper around the original BtrBlocks library.
## Library Features
- Column compression and decompression with the supported data types (integer, float64, and string).
- [DataFusion](https://datafusion.apache.org/) [TableProvider](https://datafusion.apache.org/library-user-guide/custom-table-providers.html) integration
- Compression and decompression into CSV format
- Implements decompression streams `ChunkedDecompressionStream` and `CsvDecompressionStream` for on-demand decompression
- Interoperability with different object stores (local filesystem, Amazon s3, Google Cloud Storage, Azure Blob Storage, and HTTP file servers)
- Mount remote BtrBlocks compressed files as a local file, offering on-demand decompression per read request

For library documentation, use `cargo`;
```bash
cargo doc --open
```
## Compilation and Testing
To compile the Rust library, the C++ project should be compiled to generate the bindings. This process is automated in the `build.rs` file, however, you need to ensure the dependencies are present in your shell for compiling the C++ BtrBlocks project. If you are using `nix`, you can just use the `flake.nix` in the project to drop yourself into a development shell;
```bash
nix develop .
```
Then, to compile the project, use `cargo`;
```bash
cargo build --release
```
After compiling the Rust library, you can run the unit tests using `cargo`;
```bash
cargo t
```
## `btr` CLI
The project also has a CLI program that uses the `btrblocks_rs` rust library under the hood and offers multiple features for interacting with the BtrBlocks format.
##### Subcommands
- `from-csv`: Compress a CSV file into btr format
- `to-csv`: Decompress a btr file into CSV format
- `mount-csv`: Mount a new file system with fuse and expose the decompressed csv file there
- `print-csv`: Decompress the btr compressed file into csv and print the result to stdout
- `query`: Run an SQL query on the given btr compressed file

##### Compile the CLI
Make sure you have the dependencies available to compile the C++ BtrBlocks library, and use cargo to compile the CLI;
```bash
cargo build --features="cli" --no-default-features --release
```
