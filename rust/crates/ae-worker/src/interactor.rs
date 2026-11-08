//! Interactive problems (Go: `interactor.go`): the submission and the
//! interactor run side by side with each one's stdout piped to the other's stdin.

use crate::checker::{PROGRAM_CPU, PROGRAM_MEMORY, PROGRAM_OUTPUT_MAX, Program};
use ae_sandbox::{
    Cmd, CmdFile, CopyIn, FileId, Language, OutputCaps, PipeIndex, PipeMap, RunInput, RunLimits,
    clock_limit, run_cmd,
};
use std::collections::BTreeMap;

/// Cmd 0 is the submission, Cmd 1 the interactor (given the test input and the
/// expected answer as files). Only the submission gets the caller's env; the
/// interactor keeps its language's env.
#[allow(clippy::too_many_arguments)]
pub fn interactive_cmds(
    lang: &Language,
    source: &str,
    file_id: Option<&FileId>,
    limits: &RunLimits,
    files: &BTreeMap<String, String>,
    env: &[String],
    caps: OutputCaps,
    it: &Program,
    input: &str,
    expected: &str,
) -> (Vec<Cmd>, Vec<PipeMap>) {
    let mut student = run_cmd(
        lang,
        &RunInput {
            source,
            stdin: "",
            file_id,
            limits: limits.clone(),
            extra_files: Some(files),
            env,
        },
        caps,
    );
    // fd 0 and 1 are the pipes.
    student.files = vec![
        CmdFile::default(),
        CmdFile::default(),
        CmdFile::collector("stderr", caps.stderr),
    ];

    let il = &it.language;
    let mut args = il.run.args.clone();
    args.extend(["/w/input.txt", "/w/expected.txt"].map(str::to_owned));
    let mut copy_in = BTreeMap::from([
        ("/w/input.txt".to_owned(), CopyIn::content(input)),
        ("/w/expected.txt".to_owned(), CopyIn::content(expected)),
    ]);
    it.mount(&mut copy_in);
    let interactor = Cmd {
        args,
        env: il.run.env.clone(),
        files: vec![
            CmdFile::default(),
            CmdFile::default(),
            CmdFile::collector("stderr", PROGRAM_OUTPUT_MAX),
        ],
        cpu_limit: PROGRAM_CPU,
        clock_limit: clock_limit(PROGRAM_CPU),
        memory_limit: PROGRAM_MEMORY,
        proc_limit: il.run.limits.procs,
        stack_limit: 0,
        copy_in,
        copy_out: vec!["stderr".into()],
        copy_out_cached: Vec::new(),
    };
    let pipes = vec![
        // Submission stdout -> interactor stdin.
        PipeMap {
            input: PipeIndex { index: 0, fd: 1 },
            output: PipeIndex { index: 1, fd: 0 },
            proxy: true,
        },
        // Interactor stdout -> submission stdin.
        PipeMap {
            input: PipeIndex { index: 1, fd: 1 },
            output: PipeIndex { index: 0, fd: 0 },
            proxy: true,
        },
    ];
    (vec![student, interactor], pipes)
}
