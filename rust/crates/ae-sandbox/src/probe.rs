//! Toolchain presence and version probes.

use crate::{Cmd, CmdFile, Error, Language, Languages, STATUS_ACCEPTED, Sandbox};
use std::collections::{BTreeMap, BTreeSet};

/// Reports, per language, whether every executable it needs exists in the
/// sandbox. One request checks each distinct executable once. An error means the
/// sandbox could not be asked, not that a language is missing.
pub async fn probe_languages(
    sb: &dyn Sandbox,
    langs: &Languages,
) -> Result<BTreeMap<String, bool>, Error> {
    let binaries: Vec<&str> = langs
        .iter()
        .flat_map(|(_, l)| l.binaries())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let cmds = binaries
        .iter()
        .map(|b| Cmd {
            args: vec!["/usr/bin/test".into(), "-x".into(), (*b).to_owned()],
            env: vec!["PATH=/usr/bin:/bin".into()],
            files: vec![
                CmdFile::stdin(""),
                CmdFile::collector("stdout", 0),
                CmdFile::collector("stderr", 0),
            ],
            cpu_limit: 1_000_000_000,
            clock_limit: 2_000_000_000,
            memory_limit: 16 << 20,
            proc_limit: 1,
            ..Cmd::default()
        })
        .collect();
    let results = sb.run(cmds).await?;
    if results.len() != binaries.len() {
        return Err(Error::ResultCount {
            got: results.len(),
            want: binaries.len(),
        });
    }
    let present: BTreeSet<&str> = binaries
        .iter()
        .zip(&results)
        .filter(|(_, r)| r.status == STATUS_ACCEPTED)
        .map(|(b, _)| *b)
        .collect();
    Ok(langs
        .iter()
        .map(|(id, l)| {
            (
                id.to_owned(),
                l.binaries().iter().all(|b| present.contains(b)),
            )
        })
        .collect())
}

/// First line of the compiler's version output (stdout, else stderr).
pub(crate) async fn toolchain_version(sb: &dyn Sandbox, lang: &Language) -> Option<String> {
    let cmd = Cmd {
        args: lang.compiler.clone(),
        env: lang.tool_env().to_vec(),
        files: vec![
            CmdFile::stdin(""),
            CmdFile::collector("stdout", 64 << 10),
            CmdFile::collector("stderr", 64 << 10),
        ],
        cpu_limit: 10_000_000_000,
        clock_limit: crate::clock_limit(10_000_000_000),
        memory_limit: 1 << 30,
        proc_limit: 50,
        copy_out: vec!["stdout".into(), "stderr".into()],
        ..Cmd::default()
    };
    let r = sb.run(vec![cmd]).await.ok()?.into_iter().next()?;
    if r.status != STATUS_ACCEPTED {
        return None;
    }
    ["stdout", "stderr"]
        .iter()
        .filter_map(|k| r.files.get(*k))
        .flat_map(|out| out.lines())
        .map(str::trim)
        .find(|l| !l.is_empty())
        .map(str::to_owned)
}
