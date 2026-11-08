//! Ports of internal/sandbox/{args,env,files,language}_test.go plus the Rust-only
//! language table checks.

use ae_core::Config;
use ae_sandbox::{
    CopyIn, FileId, Languages, MAX_ARG_BYTES, MAX_ARGS, MAX_ENV_TOTAL_BYTES, MAX_ENV_VALUE_BYTES,
    MAX_ENV_VARS, OutputCaps, RunInput, RunLimits, cache_source_cmd, clock_limit, compile_cmd,
    merge_env, run_cmd, validate_args, validate_compiler_flags, validate_env, validate_files,
};
use std::collections::{BTreeMap, HashMap};

fn langs() -> Languages {
    Languages::builtin()
}

fn caps() -> OutputCaps {
    OutputCaps {
        stdout: 8 << 20,
        stderr: 1 << 20,
    }
}

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| (*x).to_owned()).collect()
}

// --- args_test.go ---

#[test]
fn validate_args_cases() {
    let a = "a".repeat(MAX_ARG_BYTES);
    let cases: Vec<(&str, Vec<String>, bool)> = vec![
        ("none", vec![], true),
        ("plain", s(&["-n", "5", "file name"]), true),
        ("too many", vec![String::new(); MAX_ARGS + 1], false),
        ("one too long", vec!["a".repeat(MAX_ARG_BYTES + 1)], false),
        ("nul byte", s(&["a\0b"]), false),
        (
            "total too large",
            vec![a.clone(), a.clone(), a.clone(), a, "a".into()],
            false,
        ),
    ];
    for (name, args, ok) in cases {
        assert_eq!(validate_args(&args).is_ok(), ok, "{name}");
    }
}

#[test]
fn run_cmd_carries_args_stack_and_procs() {
    let l = langs();
    let py = l.get("python").unwrap();
    let before = py.run.args.clone();
    let cmd = run_cmd(
        py,
        &RunInput {
            source: "print(1)",
            limits: RunLimits {
                args: s(&["a", "b"]),
                stack: 8 << 20,
                procs: 4,
                ..RunLimits::default()
            },
            ..RunInput::default()
        },
        caps(),
    );
    let mut want = before.clone();
    want.extend(s(&["a", "b"]));
    assert_eq!(cmd.args, want);
    assert_eq!((cmd.stack_limit, cmd.proc_limit), (8 << 20, 4));
    assert_eq!(py.run.args, before);
    let plain = run_cmd(
        py,
        &RunInput {
            source: "print(1)",
            ..RunInput::default()
        },
        caps(),
    );
    assert_eq!(plain.proc_limit, py.run.limits.procs);
    assert_eq!(plain.stack_limit, 0);
}

#[test]
fn java_starts_lean_and_keeps_its_executables() {
    let l = langs();
    for id in ["java", "kotlin"] {
        let run = &l.get(id).unwrap().run.args;
        assert_eq!(run[0], "/usr/bin/java", "{id}");
        for flag in [
            "-XX:+UseSerialGC",
            "-Xshare:auto",
            "-XX:TieredStopAtLevel=1",
        ] {
            assert!(run.iter().any(|a| a == flag), "{id} lacks {flag}");
        }
    }
    let javac = &l.get("java").unwrap().compile.as_ref().unwrap().args;
    assert_eq!(javac[0], "/usr/bin/javac");
    assert!(javac[1].starts_with("-J"));
}

// --- env_test.go ---

#[test]
fn validate_env_cases() {
    let l = langs();
    let too_many: Vec<String> = (0..=MAX_ENV_VARS).map(|i| format!("K{i}=v")).collect();
    let half = "x".repeat(MAX_ENV_TOTAL_BYTES / 2);
    let mut cases: Vec<(String, Vec<String>, bool)> = vec![
        ("nil".into(), vec![], false),
        ("tz".into(), s(&["TZ=UTC", "LANG=en_US.UTF-8"]), false),
        ("flag".into(), s(&["AC_MODE=strict", "_flag=1"]), false),
        ("empty value".into(), s(&["EMPTY="]), false),
        ("equals in value".into(), s(&["EXPR=a=b"]), false),
        ("no equals".into(), s(&["TZ"]), true),
        ("empty key".into(), s(&["=v"]), true),
        ("digit".into(), s(&["1TZ=UTC"]), true),
        ("dash".into(), s(&["MY-VAR=1"]), true),
        ("nul".into(), s(&["TZ=UTC\0LD_PRELOAD=x"]), true),
        ("dup".into(), s(&["TZ=UTC", "TZ=EST"]), true),
        ("too many".into(), too_many, true),
        (
            "value too long".into(),
            vec![format!("V={}", "x".repeat(MAX_ENV_VALUE_BYTES + 1))],
            true,
        ),
        (
            "total too long".into(),
            vec![format!("A={half}"), format!("B={half}")],
            true,
        ),
        ("bash fn".into(), s(&["BASH_FUNC_f%%=() { :; }"]), true),
    ];
    for k in [
        "LD_PRELOAD",
        "LD_LIBRARY_PATH",
        "GLIBC_TUNABLES",
        "MALLOC_ARENA_MAX",
        "GCONV_PATH",
        "JAVA_TOOL_OPTIONS",
        "_JAVA_OPTIONS",
        "JDK_JAVA_OPTIONS",
        "JAVA_OPTS",
        "PYTHONSTARTUP",
        "NODE_OPTIONS",
        "BUN_OPTIONS",
        "MONO_ENV_OPTIONS",
        "RUBYOPT",
        "RUBYLIB",
        "PHPRC",
        "PHP_INI_SCAN_DIR",
        "BASH_ENV",
        "BASHOPTS",
        "ENV",
        "SHELLOPTS",
        "PS4",
        "GODEBUG",
        "GOGC",
        "GOMEMLIMIT",
        "GOMAXPROCS",
        "GOTRACEBACK",
        "PATH",
        "HOME",
        "GOROOT",
        "KOTLIN_HOME",
    ] {
        cases.push((k.into(), vec![format!("{k}=/w/x")], true));
    }
    for (name, env, want_err) in cases {
        assert_eq!(validate_env(&env, &l).is_err(), want_err, "{name}");
    }
}

#[test]
fn validate_env_rejects_all_language_keys() {
    let l = langs();
    for (id, lang) in l.iter() {
        let compile = lang.compile.iter().flat_map(|c| c.env.iter());
        for kv in compile.chain(lang.run.env.iter()) {
            assert!(
                validate_env(std::slice::from_ref(kv), &l).is_err(),
                "{id}: {kv} accepted"
            );
        }
    }
}

#[test]
fn merge_env_cases() {
    let cases: Vec<(Vec<String>, Vec<String>, Vec<String>)> = vec![
        (vec![], vec![], vec![]),
        (s(&["TZ=UTC"]), vec![], s(&["TZ=UTC"])),
        (vec![], s(&["TZ=UTC"]), s(&["TZ=UTC"])),
        (s(&["TZ=UTC"]), s(&["LANG=C"]), s(&["TZ=UTC", "LANG=C"])),
        (
            s(&["TZ=UTC", "LANG=C"]),
            s(&["TZ=EST", "X=1"]),
            s(&["TZ=EST", "LANG=C", "X=1"]),
        ),
    ];
    for (base, over, want) in cases {
        assert_eq!(merge_env(&base, &over), want);
    }
}

#[test]
fn run_cmd_does_not_alias_language_env() {
    let l = langs();
    let py = l.get("python").unwrap();
    let before = py.run.env.clone();
    let id = FileId("file-id".into());
    let mk = |job: &str| {
        let env = vec![format!("JOB={job}")];
        run_cmd(
            py,
            &RunInput {
                file_id: Some(&id),
                env: &env,
                ..RunInput::default()
            },
            caps(),
        )
    };
    let (a, b) = (mk("a"), mk("b"));
    assert_eq!(a.env.last().unwrap(), "JOB=a");
    assert_eq!(b.env.last().unwrap(), "JOB=b");
    assert_eq!(py.run.env, before);
}

// --- files_test.go ---

#[test]
fn validate_compiler_flags_cases() {
    let l = langs();
    let cases: Vec<(&str, Vec<String>, bool)> = vec![
        ("cpp", s(&["-O3", "-std=c++20"]), false),
        ("c", s(&["-O2", "-Wall"]), false),
        ("c", s(&["-O3", "-std=c11", "-lm"]), false),
        ("rust", s(&["-O", "--edition=2021"]), false),
        ("cpp", s(&["-funsafe-flag"]), true),
        ("python", s(&["-O3"]), true),
        ("cpp", s(&["-I/etc/shadow"]), true),
        ("python", vec![], false),
    ];
    for (id, flags, want_err) in cases {
        let lang = l.get(id).unwrap();
        assert_eq!(
            validate_compiler_flags(lang, &flags).is_err(),
            want_err,
            "{id} {flags:?}"
        );
    }
}

fn files(v: &[(&str, &str)]) -> BTreeMap<String, String> {
    v.iter()
        .map(|(k, c)| ((*k).to_owned(), (*c).to_owned()))
        .collect()
}

#[test]
fn validate_files_cases() {
    let valid = files(&[("solution.h", "#pragma once"), ("util.cpp", "int u();")]);
    assert!(validate_files(&valid, 10, 128 << 10).is_ok());
    let too_many: BTreeMap<String, String> = (0..11)
        .map(|i| (format!("{i}.h"), "ok".to_owned()))
        .collect();
    assert!(validate_files(&too_many, 10, 128 << 10).is_err());
    for bad in ["../secret.txt", "/etc/hosts", "..", ".", "a/../../b", ""] {
        assert!(
            validate_files(&files(&[(bad, "x")]), 10, 1 << 20).is_err(),
            "{bad:?} accepted"
        );
    }
    assert!(validate_files(&files(&[("a/../b.h", "x")]), 10, 1 << 20).is_ok());
    assert!(validate_files(&files(&[("a.h", "xx"), ("b.h", "xx")]), 10, 3).is_err());
}

// --- language_test.go ---

#[test]
fn effective_defaults_without_limits() {
    let l = langs();
    let cpp = l.get("cpp").unwrap();
    assert_eq!(
        cpp.effective(&RunLimits::default()),
        (cpp.run.limits.cpu, cpp.run.limits.memory)
    );
}

#[test]
fn effective_applies_request_limits() {
    let l = langs();
    let got = l.get("cpp").unwrap().effective(&RunLimits {
        cpu: 1_000_000_000,
        memory: 64 << 20,
        ..RunLimits::default()
    });
    assert_eq!(got, (1_000_000_000, 64 << 20));
}

#[test]
fn effective_keeps_jvm_memory_floor() {
    let l = langs();
    let java = l.get("java").unwrap();
    let (_, mem) = java.effective(&RunLimits {
        memory: 32 << 20,
        ..RunLimits::default()
    });
    assert_eq!(mem, java.run.limits.min_memory);
    assert_eq!(mem, 268_435_456);
}

#[test]
fn run_cmd_sets_limits_and_clock() {
    let l = langs();
    let cmd = run_cmd(
        l.get("python").unwrap(),
        &RunInput {
            source: "print(1)",
            limits: RunLimits {
                cpu: 3_000_000_000,
                memory: 128 << 20,
                ..RunLimits::default()
            },
            ..RunInput::default()
        },
        caps(),
    );
    assert_eq!(
        (cmd.cpu_limit, cmd.memory_limit, cmd.clock_limit),
        (3_000_000_000, 128 << 20, 7_000_000_000)
    );
    assert_eq!(clock_limit(2_000_000_000), 5_000_000_000);
}

#[test]
fn rust_compiles_as_edition_2021() {
    let l = langs();
    let rust = l.get("rust").unwrap();
    let rustc = s(&["/usr/bin/rustc", "-O", "-o", "/w/a.out", "/w/main.rs"]);
    let with = |extra: &[&str]| {
        let mut v = rustc.clone();
        v.extend(s(extra));
        v
    };
    let cases = [
        (vec![], with(&["--edition=2021"])),
        (s(&["--edition=2018"]), with(&["--edition=2018"])),
        (s(&["--edition=2021"]), with(&["--edition=2021"])),
        (s(&["-O"]), with(&["--edition=2021", "-O"])),
    ];
    for (flags, want) in cases {
        let cmd = compile_cmd(rust, "fn main(){}", &BTreeMap::new(), &flags).unwrap();
        assert_eq!(cmd.args, want, "{flags:?}");
    }
}

#[test]
fn compile_cmd_without_defaults() {
    let l = langs();
    let cpp = l.get("cpp").unwrap();
    let got = compile_cmd(cpp, "", &BTreeMap::new(), &s(&["-O3"])).unwrap();
    let mut want = cpp.compile.as_ref().unwrap().args.clone();
    want.push("-O3".into());
    assert_eq!(got.args, want);
    assert!(compile_cmd(l.get("python").unwrap(), "", &BTreeMap::new(), &[]).is_none());
}

#[test]
fn compile_cmd_never_repeats_allowed_flag() {
    let l = langs();
    for (id, lang) in l.iter() {
        let Some(c) = &lang.compile else { continue };
        for flag in &lang.allowed_flags {
            let args = compile_cmd(lang, "", &BTreeMap::new(), std::slice::from_ref(flag))
                .unwrap()
                .args;
            let mut seen = std::collections::HashSet::new();
            for a in &args[c.args.len()..] {
                let name = a.split('=').next().unwrap();
                assert!(seen.insert(name.to_owned()), "{id} {flag}: {args:?}");
            }
        }
    }
}

#[test]
fn compile_cmd_shape_matches_go() {
    let l = langs();
    let c = l.get("c").unwrap();
    let cmd = compile_cmd(c, "int main(){}", &files(&[("u.h", "x")]), &[]).unwrap();
    assert_eq!(cmd.env, s(&["PATH=/usr/bin:/bin"]));
    assert_eq!(
        (
            cmd.cpu_limit,
            cmd.clock_limit,
            cmd.memory_limit,
            cmd.proc_limit
        ),
        (10_000_000_000, 21_000_000_000, 268_435_456, 50)
    );
    assert_eq!(cmd.files[1].max, Some(64 << 10));
    assert_eq!(cmd.files[2].max, Some(64 << 10));
    assert_eq!(cmd.copy_out, s(&["stdout", "stderr"]));
    assert_eq!(cmd.copy_out_cached, s(&["/w/a.out"]));
    assert_eq!(
        cmd.copy_in.get("/w/main.c"),
        Some(&CopyIn::content("int main(){}"))
    );
    assert_eq!(cmd.copy_in.get("/w/u.h"), Some(&CopyIn::content("x")));
}

#[test]
fn run_cmd_file_placement() {
    let l = langs();
    let id = FileId("abc".into());
    let c = run_cmd(
        l.get("c").unwrap(),
        &RunInput {
            file_id: Some(&id),
            stdin: "in",
            ..RunInput::default()
        },
        caps(),
    );
    assert_eq!(c.copy_in.get("/w/a.out"), Some(&CopyIn::file_id("abc")));
    assert_eq!(c.files[0].content.as_deref(), Some("in"));
    let py = run_cmd(
        l.get("python").unwrap(),
        &RunInput {
            file_id: Some(&id),
            ..RunInput::default()
        },
        caps(),
    );
    assert_eq!(py.copy_in.get("/w/main.py"), Some(&CopyIn::file_id("abc")));
    let inline = run_cmd(
        l.get("python").unwrap(),
        &RunInput {
            source: "print(1)",
            ..RunInput::default()
        },
        caps(),
    );
    assert_eq!(
        inline.copy_in.get("/w/main.py"),
        Some(&CopyIn::content("print(1)"))
    );
    let sql = run_cmd(
        l.get("sqlite").unwrap(),
        &RunInput {
            source: "SELECT 1;",
            stdin: "ignored",
            ..RunInput::default()
        },
        caps(),
    );
    assert_eq!(sql.files[0].content.as_deref(), Some("SELECT 1;"));
    assert!(sql.copy_in.is_empty());
}

#[test]
fn cache_source_cmd_shape() {
    let l = langs();
    let cmd = cache_source_cmd(l.get("python").unwrap(), "print(1)").unwrap();
    assert_eq!(cmd.args, s(&["/bin/true"]));
    assert!(cmd.env.is_empty());
    assert_eq!(
        (
            cmd.cpu_limit,
            cmd.clock_limit,
            cmd.memory_limit,
            cmd.proc_limit
        ),
        (1_000_000_000, 2_000_000_000, 4 << 20, 1)
    );
    assert_eq!(cmd.copy_out_cached, s(&["/w/main.py"]));
    assert!(cache_source_cmd(l.get("sqlite").unwrap(), "").is_none());
}

#[test]
fn parallelism_hints_by_language() {
    let l = langs();
    for id in ["c", "cpp", "rust"] {
        assert_eq!(l.get(id).unwrap().parallelism, None, "{id}");
    }
    for id in ["python", "javascript"] {
        assert_eq!(l.get(id).unwrap().parallelism, Some(8), "{id}");
    }
    for id in ["java", "csharp"] {
        assert_eq!(l.get(id).unwrap().parallelism, Some(4), "{id}");
    }
}

#[test]
fn stdout_truncated_at_ac_sandbox_stdout_max_bytes() {
    let m: HashMap<String, String> = [
        ("AC_SANDBOX_STDOUT_MAX_BYTES", "1234"),
        ("AC_SANDBOX_STDERR_MAX_BYTES", "99"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();
    let cfg = Config::from_map(&m).unwrap();
    let caps = OutputCaps::from_config(&cfg);
    let l = langs();
    let cmd = run_cmd(l.get("python").unwrap(), &RunInput::default(), caps);
    assert_eq!(cmd.files[1].name.as_deref(), Some("stdout"));
    assert_eq!(cmd.files[1].max, Some(1234));
    assert_eq!(cmd.files[2].name.as_deref(), Some("stderr"));
    assert_eq!(cmd.files[2].max, Some(99));
}

#[test]
fn injection_attempts_stay_single_argv_entries() {
    let l = langs();
    let evil = s(&["$(id)", "; rm -rf /", "`whoami`"]);
    let cmd = run_cmd(
        l.get("bash").unwrap(),
        &RunInput {
            source: "echo hi",
            limits: RunLimits {
                args: evil.clone(),
                ..RunLimits::default()
            },
            ..RunInput::default()
        },
        caps(),
    );
    assert_eq!(&cmd.args[..2], &s(&["/bin/bash", "/w/main.sh"])[..]);
    assert_eq!(&cmd.args[2..], &evil[..]);
    // A file name with shell metacharacters is a copyIn key only, never an argument.
    let name = "x; rm -rf $(pwd).h";
    let cc = compile_cmd(l.get("c").unwrap(), "", &files(&[(name, "x")]), &[]).unwrap();
    assert!(cc.copy_in.contains_key(&format!("/w/{name}")));
    assert!(!cc.args.iter().any(|a| a.contains("rm")));
    // A flag smuggling a second option is rejected, not split.
    let c = l.get("c").unwrap();
    assert!(validate_compiler_flags(c, &s(&["-O2 -I/etc"])).is_err());
}

#[test]
fn custom_table_overrides_builtin() {
    let toml = r#"
[languages.py]
compiler = ["/usr/bin/python3", "--version"]
parallelism = 2
overlay = { source = "/w/main.py" }
hello = 'print("hello")'
[languages.py.execute_cmd]
args = ["/usr/bin/python3", "/w/main.py"]
env = ["PATH=/usr/bin"]
limits = { cpu = 1_000_000_000, memory = 67_108_864, procs = 5 }
"#;
    let l = Languages::from_toml(toml).unwrap();
    assert_eq!(l.iter().count(), 1);
    assert_eq!(l.get("py").unwrap().parallelism, Some(2));
    assert!(l.get("python").is_none());

    let dir = std::env::temp_dir().join(format!("ae-sandbox-{}.toml", std::process::id()));
    std::fs::write(&dir, toml).unwrap();
    let from_file = Languages::load(dir.to_str().unwrap()).unwrap();
    std::fs::remove_file(&dir).unwrap();
    assert!(from_file.get("py").is_some());
    assert!(Languages::load("").unwrap().get("python").is_some());

    for bad in [
        "[languages.x]\ncompiler=[]\n",
        &toml.replace("parallelism = 2", "parallelism = 0"),
        &toml.replace("/w/main.py\" }", "/etc/main.py\" }"),
        &toml.replace("args = [\"/usr/bin/python3\", \"/w/main.py\"]", "args = []"),
        &toml.replace("env = [\"PATH=/usr/bin\"]", "env = [\"PATH\"]"),
    ] {
        assert!(Languages::from_toml(bad).is_err(), "{bad}");
    }
}
