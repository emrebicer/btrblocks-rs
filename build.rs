use std::path::{Path, PathBuf};

fn main() {
    let out_dir: PathBuf = std::env::var_os("OUT_DIR")
        .expect("OUT_DIR environment variable must be set")
        .into();

    // Clone btrblocks
    let btrblocks_url = "https://github.com/emrebicer/btrblocks/";
    let btrblocks_source = out_dir.join(Path::new("btrblocks"));
    if !btrblocks_source.exists() {
        git2::build::RepoBuilder::new()
            .branch("lib-build-only-test")
            .clone(btrblocks_url, btrblocks_source.as_path())
            .expect("failed to clone git repo");
    }

    
    let dst = cmake::Config::new(btrblocks_source.clone())
        .define("CMAKE_CXX_STANDARD", "17")
        //.very_verbose(true)
        .build();


    println!("cmake dst is: {:#?}", dst);

    // Set up the include and lib paths for linking
    println!("cargo:rustc-link-search=native={}/build", dst.display());
    //println!("cargo:rustc-link-search=native={}/btrblocks", dst.display());
    println!("cargo:rustc-link-lib=static=btrblocks");


    // Generate CXX bindings
    cxx_build::bridge("src/main.rs")
        .include(btrblocks_source.join("btrblocks"))
        .include(dst.join("build//vendor/croaring/include"))
        .flag_if_supported("-std=c++17")
        .flag_if_supported("-pthread")
        .flag_if_supported("-march=native")
        .flag_if_supported("-Wall")
        .flag_if_supported("-Wextra")
        .compile("btrblocks-rust");

    println!("cargo:rerun-if-changed=src/main.rs");
}
