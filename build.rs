fn main() {
    #[cfg(feature = "grpc")]
    {
        let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());

        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .out_dir(&out_dir)
            .compile_protos(&["proto/raft.proto"], &["proto"])
            .expect("Failed to compile protobuf");

        println!("cargo:rerun-if-changed=proto/raft.proto");
    }
}
