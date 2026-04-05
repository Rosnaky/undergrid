fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo::rerun-if-changed=../../proto/undergrid.proto");

    tonic_prost_build::compile_protos("../../proto/undergrid.proto")?;
    Ok(())
}
