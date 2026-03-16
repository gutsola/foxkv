win上运行:
$env:FOXKV_THREADS=16; cargo run --release --bin foxkv

编译到Linux(默认分配器):
$env:RUSTFLAGS='-Clinker=rust-lld'; cargo build --release --bin foxkv --target x86_64-unknown-linux-musl