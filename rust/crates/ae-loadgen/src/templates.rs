use rand::Rng;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct TestInput {
    pub input: &'static str,
    pub expected_output: &'static str,
}

#[derive(Debug, Clone)]
pub struct Template {
    pub language: &'static str,
    pub source: &'static str,
    pub tests: &'static [TestInput],
    pub expected_verdict: &'static str,
}

const fn t(input: &'static str, expected_output: &'static str) -> TestInput {
    TestInput {
        input,
        expected_output,
    }
}

const DOUBLE: &[TestInput] = &[t("21\n", "42\n"), t("5\n", "10\n")];
const DOUBLE1: &[TestInput] = &[t("21\n", "42\n")];
const EMPTY_DONE: &[TestInput] = &[t("", "done\n")];
const EMPTY_42: &[TestInput] = &[t("", "42\n")];
const EMPTY_1: &[TestInput] = &[t("", "1\n")];

const fn tpl(
    language: &'static str,
    source: &'static str,
    tests: &'static [TestInput],
    expected_verdict: &'static str,
) -> Template {
    Template {
        language,
        source,
        tests,
        expected_verdict,
    }
}

/// Go's submission mix: (weight out of 1000, template). The weights are used within a
/// language; across languages the mix is uniform over the requested list.
const TEMPLATES: &[(u32, Template)] = &[
    (
        360,
        tpl("python", "n=int(input())\nprint(n*2)", DOUBLE, "accepted"),
    ),
    (
        20,
        tpl(
            "python",
            "n=int(input())\nprint(n+1)",
            &[t("5\n", "10\n")],
            "wrong_answer",
        ),
    ),
    (
        10,
        tpl("python", "while True: pass", EMPTY_DONE, "time_limit"),
    ),
    (
        10,
        tpl("python", "print(1/0)", &[t("", "0\n")], "runtime_error"),
    ),
    (
        180,
        tpl(
            "c",
            "#include <stdio.h>\nint main() { int n; scanf(\"%d\", &n); printf(\"%d\\n\", n*2); return 0; }",
            DOUBLE,
            "accepted",
        ),
    ),
    (
        10,
        tpl(
            "c",
            "int main() { undeclared = 42; }",
            EMPTY_42,
            "compilation_error",
        ),
    ),
    (
        10,
        tpl(
            "c",
            "#include <stdio.h>\nint main() { while(1); return 0; }",
            EMPTY_DONE,
            "time_limit",
        ),
    ),
    (
        140,
        tpl(
            "cpp",
            "#include <iostream>\nusing namespace std;\nint main() { int n; cin >> n; cout << n*2 << endl; return 0; }",
            DOUBLE1,
            "accepted",
        ),
    ),
    (
        10,
        tpl(
            "cpp",
            "#include <iostream>\nint main() { undeclared = 1; }",
            EMPTY_1,
            "compilation_error",
        ),
    ),
    (
        140,
        tpl(
            "java",
            "import java.util.Scanner;\npublic class Main {\n  public static void main(String[] args) {\n    Scanner sc = new Scanner(System.in);\n    int n = sc.nextInt();\n    System.out.println(n * 2);\n  }\n}",
            DOUBLE1,
            "accepted",
        ),
    ),
    (
        10,
        tpl(
            "java",
            "public class Main { public static void main(String[] args) { undeclared = 1; } }",
            EMPTY_1,
            "compilation_error",
        ),
    ),
    (
        50,
        tpl(
            "javascript",
            "const readline = require(\"readline\");\nconst rl = readline.createInterface({ input: process.stdin });\nrl.on(\"line\", (line) => { console.log(parseInt(line) * 2); rl.close(); });",
            DOUBLE1,
            "accepted",
        ),
    ),
    (
        20,
        tpl(
            "sqlite",
            "CREATE TABLE t(x INTEGER);\nINSERT INTO t VALUES(1),(2),(3);\nSELECT SUM(x) FROM t;",
            &[t("", "6\n")],
            "accepted",
        ),
    ),
    (
        30,
        tpl(
            "go",
            "package main\nimport \"fmt\"\nfunc main() {\n\tvar n int\n\tfmt.Scan(&n)\n\tfmt.Println(n * 2)\n}",
            DOUBLE1,
            "accepted",
        ),
    ),
];

/// Languages that have templates.
#[must_use]
pub fn supported() -> Vec<&'static str> {
    let mut v: Vec<&str> = Vec::new();
    for (_, t) in TEMPLATES {
        if !v.contains(&t.language) {
            v.push(t.language);
        }
    }
    v
}

/// A random template: uniform over `languages`, weighted by the Go mix within the language.
///
/// # Panics
/// When `languages` is empty or names a language without templates (the CLI validates).
pub fn pick<R: Rng>(rng: &mut R, languages: &[String]) -> Template {
    let lang = &languages[rng.gen_range(0..languages.len())];
    let pool: Vec<&(u32, Template)> = TEMPLATES
        .iter()
        .filter(|(_, t)| t.language == lang)
        .collect();
    let total: u32 = pool.iter().map(|(w, _)| w).sum();
    let mut r = rng.gen_range(0..total);
    for (w, t) in &pool {
        if r < *w {
            return t.clone();
        }
        r -= w;
    }
    pool[0].1.clone()
}
