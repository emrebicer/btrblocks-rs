fn main() {
    println!("Hello, world!");

    ffi::emretestfunc();
}

#[cxx::bridge(namespace = "btrblocks")]
mod ffi {
    unsafe extern "C++" {
        include!("btrblocks.hpp");

        fn emretestfunc();
    }
}





