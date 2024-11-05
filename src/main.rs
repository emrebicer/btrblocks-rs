use std::os::raw::c_int;

fn main() {
    println!("Hello, world!");

    ffi::emretestfunc(3);
    ffi::configure_btrblocks(3);
}

#[cxx::bridge(namespace = "btrblocks")]
mod ffi {
    unsafe extern "C++" {
        include!("btrblocks.hpp");

        fn emretestfunc(value: i32);

        fn configure_btrblocks(max_depth: u32);
    }
}
