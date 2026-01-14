fn main() {
    #[cfg(feature = "grpc")]
    {
        if std::env::var("PROTOC").is_err() {
            unsafe {
                std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
            }
        }

        let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());

        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .out_dir(&out_dir)
            .compile(&["proto/raft.proto"], &["proto"])
            .expect("Failed to compile protobuf");

        println!("cargo:rerun-if-changed=proto/raft.proto");
    }
}
