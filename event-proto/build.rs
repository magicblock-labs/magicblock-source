fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=proto/event.proto");

    let mut config = prost_build::Config::new();
    config.boxed(".blockdaemon.solana.accountsdb_plugin_kafka.types.MessageWrapper");
    config.protoc_arg("--experimental_allow_proto3_optional");
    config
        .compile_protos(&["proto/event.proto"], &["proto/"])
        .expect("failed to compile protobuf definitions");
}
