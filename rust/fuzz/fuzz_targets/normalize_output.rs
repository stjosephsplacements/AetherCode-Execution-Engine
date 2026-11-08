#![no_main]
use ae_core::judge::normalize_output;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let once = normalize_output(data);
    assert_eq!(normalize_output(&once), once, "normalize_output is not idempotent");
});
