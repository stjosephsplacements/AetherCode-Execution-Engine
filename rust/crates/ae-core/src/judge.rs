use crate::model::Checker;

const FLOAT_EPSILON: f64 = 1e-6;

/// Compares `actual` against `expected` with the given checker. Both sides are raw bytes:
/// invalid UTF-8 and NUL bytes are compared bytewise and never cause a panic.
#[must_use]
pub fn judge_output(checker: Checker, actual: &[u8], expected: &[u8]) -> bool {
    match checker {
        Checker::Exact => equal_normalized(expected, actual),
        Checker::Token => judge_token(expected, actual),
        Checker::Float => judge_float(expected, actual, FLOAT_EPSILON),
    }
}

/// Start, end and following offset of the next non-blank line with trailing spaces/tabs removed.
/// Lines end at `\r\n`, `\r` or `\n`. `None` when only blank lines remain.
fn next_effective_line(s: &[u8], offset: usize) -> Option<(usize, usize, usize)> {
    let n = s.len();
    let mut cur = offset;
    while cur < n {
        let start = cur;
        let mut trailing_ws: Option<usize> = None;
        while cur < n && s[cur] != b'\n' && s[cur] != b'\r' {
            if s[cur] == b' ' || s[cur] == b'\t' {
                trailing_ws.get_or_insert(cur);
            } else {
                trailing_ws = None;
            }
            cur += 1;
        }
        let end = trailing_ws.unwrap_or(cur);
        if cur < n {
            if s[cur] == b'\r' {
                cur += 1;
                if cur < n && s[cur] == b'\n' {
                    cur += 1;
                }
            } else {
                cur += 1;
            }
        }
        if end > start {
            return Some((start, end, cur));
        }
    }
    None
}

fn equal_normalized(expected: &[u8], actual: &[u8]) -> bool {
    let (mut eo, mut ao) = (0, 0);
    loop {
        match (
            next_effective_line(expected, eo),
            next_effective_line(actual, ao),
        ) {
            (None, None) => return true,
            (Some((es, ee, en)), Some((as_, ae, an))) => {
                if expected[es..ee] != actual[as_..ae] {
                    return false;
                }
                eo = en;
                ao = an;
            }
            _ => return false,
        }
    }
}

const fn is_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

/// Next whitespace-delimited token as `(start, end)`.
fn next_token(s: &[u8], offset: usize) -> Option<(usize, usize)> {
    let mut i = offset;
    while i < s.len() && is_ws(s[i]) {
        i += 1;
    }
    if i >= s.len() {
        return None;
    }
    let start = i;
    while i < s.len() && !is_ws(s[i]) {
        i += 1;
    }
    Some((start, i))
}

fn judge_token(expected: &[u8], actual: &[u8]) -> bool {
    let (mut eo, mut ao) = (0, 0);
    loop {
        match (next_token(expected, eo), next_token(actual, ao)) {
            (None, None) => return true,
            (Some((es, ee)), Some((as_, ae))) => {
                if expected[es..ee] != actual[as_..ae] {
                    return false;
                }
                eo = ee;
                ao = ae;
            }
            _ => return false,
        }
    }
}

fn judge_float(expected: &[u8], actual: &[u8], epsilon: f64) -> bool {
    let (mut eo, mut ao) = (0, 0);
    loop {
        match (next_token(expected, eo), next_token(actual, ao)) {
            (None, None) => return true,
            (Some((es, ee)), Some((as_, ae))) => {
                let (et, at) = (&expected[es..ee], &actual[as_..ae]);
                match (parse_float(et), parse_float(at)) {
                    (Some(e), Some(a)) => {
                        if e.is_nan() || a.is_nan() || (e - a).abs() > epsilon {
                            return false;
                        }
                    }
                    _ => {
                        if et != at {
                            return false;
                        }
                    }
                }
                eo = ee;
                ao = ae;
            }
            _ => return false,
        }
    }
}

/// Parses a token like Go's `strconv.ParseFloat(tok, 64)` and returns `None` where Go
/// returns an error: syntax errors and out-of-range magnitudes (`1e999`). Accepts decimal
/// and hexadecimal floats, `inf`/`infinity`/`nan` in any case.
fn parse_float(tok: &[u8]) -> Option<f64> {
    let (neg, body) = match tok.first()? {
        b'+' => (false, &tok[1..]),
        b'-' => (true, &tok[1..]),
        _ => (false, tok),
    };
    // Go accepts one sign only; "--5" is a syntax error and compares as text.
    if matches!(body.first(), Some(b'+' | b'-')) {
        return None;
    }
    let v = if body.len() > 2 && body[0] == b'0' && body[1].eq_ignore_ascii_case(&b'x') {
        parse_hex(&body[2..])?
    } else if body.eq_ignore_ascii_case(b"inf") || body.eq_ignore_ascii_case(b"infinity") {
        return Some(if neg {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    } else if body.eq_ignore_ascii_case(b"nan") {
        // Go accepts only an unsigned NaN; "-nan" is a syntax error and compares as text.
        return if tok.len() == body.len() {
            Some(f64::NAN)
        } else {
            None
        };
    } else {
        let t = std::str::from_utf8(body).ok()?;
        if !t
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-'))
        {
            return None;
        }
        let v: f64 = t.parse().ok()?;
        if v.is_infinite() {
            return None;
        }
        v
    };
    Some(if neg { -v } else { v })
}

/// Hexadecimal float body after `0x`: digits, optional `.`, mandatory `p` exponent.
/// Underscores are allowed between digits. `None` on syntax error or overflow.
fn parse_hex(s: &[u8]) -> Option<f64> {
    let mut i = 0;
    let mut mant: u64 = 0;
    let mut dropped_exp: i64 = 0;
    let mut seen_dot = false;
    let mut digits = 0;
    let mut prev_us = true; // underscore may directly follow the prefix
    while i < s.len() {
        let c = s[i];
        match c {
            b'_' => {
                if prev_us && digits > 0 {
                    return None;
                }
                prev_us = true;
            }
            b'.' => {
                if seen_dot {
                    return None;
                }
                seen_dot = true;
                prev_us = false;
            }
            _ => {
                let Some(d) = (c as char).to_digit(16) else {
                    break;
                };
                digits += 1;
                prev_us = false;
                if mant >> 60 == 0 {
                    mant = mant << 4 | u64::from(d);
                    if seen_dot {
                        dropped_exp -= 4;
                    }
                } else if !seen_dot {
                    dropped_exp += 4;
                }
            }
        }
        i += 1;
    }
    if digits == 0 || prev_us && s.get(i - 1) == Some(&b'_') {
        return None;
    }
    if !s.get(i)?.eq_ignore_ascii_case(&b'p') {
        return None;
    }
    i += 1;
    let eneg = match s.get(i)? {
        b'-' => {
            i += 1;
            true
        }
        b'+' => {
            i += 1;
            false
        }
        _ => false,
    };
    let mut e: i64 = 0;
    let mut edigits = 0;
    let mut eus = false;
    while i < s.len() {
        match s[i] {
            b'_' if edigits > 0 && !eus => eus = true,
            c @ b'0'..=b'9' => {
                e = (e * 10 + i64::from(c - b'0')).min(100_000);
                edigits += 1;
                eus = false;
            }
            _ => return None,
        }
        i += 1;
    }
    if edigits == 0 || eus {
        return None;
    }
    let exp = (if eneg { -e } else { e }) + dropped_exp;
    let exp = i32::try_from(exp.clamp(-5000, 5000)).ok()?;
    // Scale in two steps so a large mantissa with a very negative exponent does not underflow early.
    let half = exp / 2;
    #[allow(clippy::cast_precision_loss)]
    let v = mant as f64 * 2f64.powi(half) * 2f64.powi(exp - half);
    if v.is_infinite() { None } else { Some(v) }
}

/// Trims trailing spaces/tabs per line and converts `\r\n` and `\r` to `\n`. Leading and
/// trailing blank lines are dropped; interior blank lines are kept.
#[must_use]
pub fn normalize_output(s: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    let mut wrote_any = false;
    let mut pending_nl = 0usize;
    let mut line_start = 0;
    let mut trailing_ws: Option<usize> = None;

    let emit = |out: &mut Vec<u8>, line: &[u8], wrote_any: &mut bool, pending_nl: &mut usize| {
        if line.is_empty() {
            if *wrote_any {
                *pending_nl += 1;
            }
            return;
        }
        if *wrote_any {
            out.extend(std::iter::repeat_n(b'\n', *pending_nl + 1));
        }
        *pending_nl = 0;
        out.extend_from_slice(line);
        *wrote_any = true;
    };

    let mut i = 0;
    while i < s.len() {
        match s[i] {
            c @ (b'\r' | b'\n') => {
                emit(
                    &mut out,
                    &s[line_start..trailing_ws.unwrap_or(i)],
                    &mut wrote_any,
                    &mut pending_nl,
                );
                trailing_ws = None;
                if c == b'\r' && s.get(i + 1) == Some(&b'\n') {
                    i += 1;
                }
                line_start = i + 1;
            }
            b' ' | b'\t' => {
                trailing_ws.get_or_insert(i);
            }
            _ => trailing_ws = None,
        }
        i += 1;
    }
    emit(
        &mut out,
        &s[line_start..trailing_ws.unwrap_or(s.len())],
        &mut wrote_any,
        &mut pending_nl,
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use Checker::{Exact, Float, Token};

    fn j(c: Checker, expected: &str, actual: &str) -> bool {
        judge_output(c, actual.as_bytes(), expected.as_bytes())
    }

    #[test]
    fn exact_cases() {
        let cases: &[(&str, &str, &str, bool)] = &[
            ("exact match", "42\n", "42\n", true),
            ("trailing newline stripped", "42", "42\n", true),
            ("trailing spaces", "42   ", "42", true),
            ("multiple trailing newlines", "42\n\n\n", "42", true),
            ("line-level trailing ws", "a  \nb\t\n", "a\nb\n", true),
            ("crlf line endings", "42\r\n", "42\n", true),
            ("empty vs empty", "", "", true),
            ("different values", "42", "43", false),
            ("empty vs content", "", "x", false),
            ("internal newlines preserved", "a\nb\nc", "a\nb\nd", false),
        ];
        for (name, e, a, want) in cases {
            assert_eq!(j(Exact, e, a), *want, "{name}");
        }
    }

    #[test]
    fn normalize_cases() {
        let cases: &[(&str, &str)] = &[
            ("a  \n b  \n", "a\n b"),
            ("line\n\n\n", "line"),
            ("pad\t\t", "pad"),
            ("a\r\nb\r\n", "a\nb"),
            ("no-change", "no-change"),
            ("", ""),
        ];
        for (i, want) in cases {
            assert_eq!(normalize_output(i.as_bytes()), want.as_bytes(), "{i:?}");
        }
    }

    #[test]
    fn carriage_returns_normalise() {
        assert!(j(Exact, "1\n2\n", "1\r\n2\r\n"));
        assert!(j(Exact, "1\n2", "1\r2"));
    }

    #[test]
    fn crlf_and_cr_normalise_to_lf() {
        assert_eq!(normalize_output(b"a\r\nb\rc\nd"), b"a\nb\nc\nd");
        assert_eq!(normalize_output(b"a\r\r\nb"), b"a\n\nb");
    }

    #[test]
    fn trailing_blank_lines_ignored() {
        assert!(j(Exact, "a\nb", "a\nb\n\n\n  \n\t\n"));
        // Like Go, the exact judge also ignores interior blank lines; normalize_output keeps them.
        assert!(j(Exact, "a\nb", "a\n\n\nb"));
        assert_eq!(normalize_output(b"\n\na\n\n\nb\n\n"), b"a\n\n\nb");
    }

    #[test]
    fn token_cases() {
        let cases: &[(&str, &str, &str, bool)] = &[
            ("exact same", "1 2 3", "1 2 3", true),
            ("extra whitespace in actual", "1 2 3", "1  2  3", true),
            ("extra newlines", "1\n2\n3\n", "1 2 3", true),
            ("mixed whitespace", "hello\tworld", "hello world", true),
            ("wrong token", "1 2 3", "1 2 4", false),
            ("too few tokens", "1 2 3", "1 2", false),
            ("too many tokens", "1 2", "1 2 3", false),
            ("empty both", "", "", true),
        ];
        for (name, e, a, want) in cases {
            assert_eq!(j(Token, e, a), *want, "{name}");
        }
    }

    #[test]
    fn float_cases() {
        let cases: &[(&str, &str, &str, bool)] = &[
            ("exact float", "3.14", "3.14", true),
            ("within epsilon", "1.0", "1.0000001", true),
            ("outside epsilon", "1.0", "1.01", false),
            ("multiple floats pass", "1.0 2.0 3.0", "1.0 2.0 3.0", true),
            ("multiple floats one fails", "1.0 2.0", "1.0 3.0", false),
            ("non-float exact match", "YES", "YES", true),
            ("non-float mismatch", "YES", "NO", false),
            ("token count mismatch", "1.0 2.0", "1.0", false),
            ("negative numbers", "-1.5", "-1.5", true),
        ];
        for (name, e, a, want) in cases {
            assert_eq!(j(Float, e, a), *want, "{name}");
        }
    }

    #[test]
    fn float_nan_never_matches() {
        for (e, a) in [
            ("3.14", "NaN"),
            ("NaN", "3.14"),
            ("NaN", "NaN"),
            ("1.0 2.0", "1.0 NaN"),
            ("nan", "nan"),
        ] {
            assert!(!j(Float, e, a), "{e} vs {a}");
        }
    }

    #[test]
    fn float_inf_equals_inf_only_if_both_same() {
        assert!(j(Float, "+Inf", "+Inf"));
        assert!(j(Float, "-Inf", "-Inf"));
        assert!(!j(Float, "+Inf", "-Inf"));
        assert!(!j(Float, "Inf", "1e308"));
        assert!(!j(Float, "1.0", "Inf"));
    }

    #[test]
    fn float_negative_zero_and_huge_exponents() {
        assert!(j(Float, "0", "-0"));
        assert!(j(Float, "-0.0", "0"));
        // Out-of-range literals are not numbers: they compare as text, never as infinity.
        assert!(j(Float, "1e999", "1e999"));
        assert!(!j(Float, "1e999", "2e999"));
        assert!(!j(Float, "1e999", "Inf"));
        assert!(!j(Float, "-1e999", "1e999"));
        assert!(j(Float, "1e-999", "0"));
        assert!(!j(Float, "1.8e308", "-1.8e308"));
    }

    #[test]
    fn non_utf8_bytes_compare_bytewise() {
        let ff = b"\xff\xfe\n";
        for c in [Exact, Token, Float] {
            assert!(judge_output(c, ff, ff));
            assert!(!judge_output(c, b"\xff\xfe\n", b"\xfe\xff\n"));
            assert!(!judge_output(c, b"\xff", b"?"));
        }
        assert!(judge_output(Exact, b"a\0b \n", b"a\0b"));
        assert!(!judge_output(Exact, b"a\0b", b"ab"));
        assert!(judge_output(Token, b"a\0b c", b"a\0b  c"));
        assert!(judge_output(Float, b"1.0 \0", b"1.0000001 \0"));
        assert!(!judge_output(Float, b"1.0 \0", b"1.0 \0\0"));
        assert_eq!(normalize_output(b"\xff \r\n\0\t\n"), b"\xff\n\0");
    }

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// Differential test against verdicts produced by the Go implementation.
    #[test]
    fn judge_matches_go_golden() {
        let g = include_str!("../testdata/judge.golden");
        let mut n = 0;
        for line in g.lines() {
            let f: Vec<&str> = line.split(' ').collect();
            let checker = Checker::from_name(f[0]);
            let (e, a) = (unhex(f[1]), unhex(f[2]));
            let want = f[3] == "accepted";
            assert_eq!(
                judge_output(checker, &a, &e),
                want,
                "{line} ({:?} vs {:?})",
                String::from_utf8_lossy(&e),
                String::from_utf8_lossy(&a)
            );
            n += 1;
        }
        assert!(n > 10_000);
    }

    #[test]
    fn doubled_sign_is_not_a_number() {
        for s in ["+-5", "--5", "-+5", "++5"] {
            assert!(!j(Float, s, "5"), "{s} vs 5");
            assert!(!j(Float, "5", s), "5 vs {s}");
            assert!(j(Float, s, s), "{s} vs itself compares as text");
        }
        assert!(!j(Float, "+-5", "-+5"));
        assert!(!j(Float, "--5", "++5"));
        let golden = include_str!("../testdata/judge.golden");
        let hex = |s: &str| s.bytes().map(|b| format!("{b:02x}")).collect::<String>();
        let line = format!("float {} {} wrong_answer", hex("--5"), hex("5"));
        assert!(
            golden.lines().any(|l| l == line),
            "golden lacks the doubled-sign case"
        );
    }

    #[test]
    fn normalize_matches_go_golden() {
        for line in include_str!("../testdata/normalize.golden").lines() {
            let (i, o) = line.split_once(' ').unwrap_or((line, ""));
            assert_eq!(normalize_output(&unhex(i)), unhex(o), "{line}");
        }
    }

    mod props {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn normalize_is_idempotent(s in proptest::collection::vec(any::<u8>(), 0..256)) {
                let n = normalize_output(&s);
                prop_assert_eq!(normalize_output(&n), n);
            }

            #[test]
            fn normalize_ascii_whitespace_heavy(s in "[ab \\t\\r\\n]{0,64}") {
                let n = normalize_output(s.as_bytes());
                prop_assert_eq!(normalize_output(&n), n.clone());
                prop_assert!(judge_output(Checker::Exact, s.as_bytes(), &n));
            }

            #[test]
            fn judge_never_panics_and_is_reflexive(
                a in proptest::collection::vec(any::<u8>(), 0..128),
                b in proptest::collection::vec(any::<u8>(), 0..128),
            ) {
                for c in [Exact, Token] {
                    let _ = judge_output(c, &a, &b);
                    prop_assert!(judge_output(c, &a, &a));
                }
                let _ = judge_output(Float, &a, &b);
            }
        }
    }
}
