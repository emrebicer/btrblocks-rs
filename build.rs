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


    // build the project using cmake
    let dst = cmake::Config::new(btrblocks_source.clone())
        .define("CMAKE_CXX_STANDARD", "17")
        .out_dir(btrblocks_source)
        .build();


    //let dst = PathBuf::from("/home/emrebicer/dev/btrblocks");
    println!("cmake dst is: {:#?}", dst);
    eprintln!("cmake dst is: {:#?}", dst);

    // Generate CXX bindings, include needed headers for btrblocks
    cxx_build::bridge("src/ffi.rs")
        .include(dst.join("btrblocks"))
        //.include(dst.join("btrblocks/btrfiles"))
        .include(dst.join("btrwrapper"))
        .include(dst.join("build/vendor/croaring/include"))
        //.include(dst.join("build/vendor/cwida/fsst/src/fsst_src"))
        //.include(dst.join("build/vendor/lemire/fastpfor/src/fastpfor_src"))
        //.include(dst.join("build/vendor/boost/boost_src-src/libs/dynamic_bitset/include"))
        //.include(dst.join("build/vendor/boost/boost_src-src/libs/assert/include"))
        //.include(dst.join("build/vendor/boost/boost_src-src/libs/config/include"))
        //.include(dst.join("build/vendor/boost/boost_src-src/libs/container_hash/include"))
        //.include(dst.join("build/vendor/boost/boost_src-src/libs/describe/include"))
        //.include(dst.join("build/vendor/boost/boost_src-src/libs/mp11/include"))
        //.include(dst.join("build/vendor/boost/boost_src-src/libs/type_traits/include"))
        //.include(dst.join("build/vendor/boost/boost_src-src/libs/static_assert/include"))
        //.include(dst.join("build/vendor/boost/boost_src-src/libs/core/include"))
        //.include(dst.join("build/vendor/boost/boost_src-src/libs/throw_exception/include"))
        //.include(dst.join("build/vendor/boost/boost_src-src/libs/integer/include"))
        //.include(dst.join("build/vendor/boost/boost_src-src/libs/move/include"))
        //.include(dst.join("build/vendor/yaml_cpp/src/yaml_src/include"))
        //.include(dst.join("build/vendor/yaml_cpp/src/yaml_src/include/yaml-cpp"))
        //.include(dst.join("build/vendor/yaml_cpp/src/yaml_src/include/yaml-cpp/contrib"))
        //.include(dst.join("build/vendor/yaml_cpp/src/yaml_src/include/yaml-cpp/node"))
        //.include(dst.join("build/vendor/yaml_cpp/src/yaml_src/include/yaml-cpp/node/detail"))
        //.include(dst.join("build/vendor/csv-parser/include"))
        .flag_if_supported("-pthread")
        .flag_if_supported("-std=gnu++17")
        .flag_if_supported("-march=native")
        .flag_if_supported("-Wall")
        .flag_if_supported("-Wextra")
        .flag_if_supported("-Wno-unused-parameter")
        .compile("btrblocks-rust");

    // Link with the dependencies
    // btrwrapper
    println!("cargo:rustc-link-search=native={}/build", dst.display());
    println!("cargo:rustc-link-lib=static=btrwrapper");

    // btrblocks
    println!("cargo:rustc-link-search=native={}/build", dst.display());
    println!("cargo:rustc-link-lib=static=btrblocks");

    // btrfiles
    //println!("cargo:rustc-link-search=native={}/build", dst.display());
    //println!("cargo:rustc-link-lib=static=btrfiles");

    // fsst
    println!("cargo:rustc-link-search=native={}", dst.join("build/vendor/cwida/fsst/src/fsst_src-build").display());
    println!("cargo:rustc-link-lib=static=fsst");

    // fastpfor
    println!("cargo:rustc-link-search=native={}", dst.join("build/vendor/lemire/fastpfor/src/fastpfor_src-build").display());
    println!("cargo:rustc-link-lib=static=FastPFOR");

    // roaring
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dst.join("build/vendor/croaring/lib").display());
    println!("cargo:rustc-link-search=native={}", dst.join("build/vendor/croaring/lib").display());
    println!("cargo:rustc-link-lib=dylib=roaring");

    // tbb
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dst.join("build/vendor/intel/tbb/src/tbb_src-build").display());
    println!("cargo:rustc-link-search=native={}", dst.join("build/vendor/intel/tbb/src/tbb_src-build").display());
    println!("cargo:rustc-link-lib=dylib=tbb");

    // yaml-cpp
    //println!("cargo:rustc-link-search=native={}", dst.join("build/vendor/yaml_cpp/src/yaml_src-build").display());
    //println!("cargo:rustc-link-lib=static=yaml-cpp");

    println!("cargo:rerun-if-changed=src/ffi.rs");
    println!("cargo:rerun-if-changed={}/build/libbtrblocks.a", dst.display());
    //println!("cargo:rerun-if-changed={}/build/libbtrfiles.a", dst.display());
    println!("cargo:rerun-if-changed={}/build/libbtrwrapper.a", dst.display());
}


