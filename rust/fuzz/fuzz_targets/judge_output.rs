#![no_main]
use ae_core::judge::judge_output;
use ae_core::model::Checker;
use libfuzzer_sys::fuzz_target;

// Input: one checker byte, then expected and actual separated by the first 0xFF 0xFF pair
// (or the middle when absent).
fuzz_target!(|data: &[u8]| {
    let Some((&c, rest)) = data.split_first() else { return };
    let checker = match c % 3 {
        0 => Checker::Exact,
        1 => Checker::Token,
        _ => Checker::Float,
    };
    let mid = rest.windows(2).position(|w| w == [0xFF, 0xFF]).unwrap_or(rest.len() / 2);
    let (expected, actual) = rest.split_at(mid);
    let _ = judge_output(checker, actual, expected);
    // Reflexive for the exact and token checkers; float is excluded because NaN never matches.
    if checker != Checker::Float {
        assert!(judge_output(checker, expected, expected));
    }
});
