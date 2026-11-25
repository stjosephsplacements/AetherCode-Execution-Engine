//! Embeds the compiler version for the health response's `go_version` field.
use std::process::Command;

fn main() {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".into());
    let v = Command::new(rustc)
        .arg("-V")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_owned())
        .unwrap_or_default();
    println!("cargo:rustc-env=AE_RUSTC_VERSION={v}");
    println!("cargo:rerun-if-env-changed=RUSTC");
}
