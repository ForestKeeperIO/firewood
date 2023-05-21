fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/proto/sync/sync.proto");

    tonic_build::compile_protos("proto/proto/sync/sync.proto")?;
    Ok(())
}