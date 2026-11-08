//! Language registry: the table plus per-language availability from a startup
//! self-test (toolchain probe, version probe, hello-world compile and run).

use crate::probe::{probe_languages, toolchain_version};
use crate::{
    Error, Language, Languages, OutputCaps, RunInput, STATUS_ACCEPTED, Sandbox, compile_cmd,
    run_cmd,
};
use ae_core::Config;
use futures_util::future::join_all;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LanguageStatus {
    pub id: String,
    pub available: bool,
}

#[derive(Debug, Clone)]
pub struct Registry {
    languages: Languages,
    /// Toolchain version of each language that passed its self-test.
    available: BTreeMap<String, String>,
    caps: OutputCaps,
}

impl Registry {
    /// Loads the table (`AC_LANGUAGES_FILE` or the built-in one) and self-tests
    /// every language. A failing language is marked unavailable; only a bad table
    /// is an error.
    pub async fn load(cfg: &Config, sb: &dyn Sandbox) -> Result<Self, Error> {
        let languages = Languages::load(&cfg.languages_file)?;
        let caps = OutputCaps::from_config(cfg);
        let present = match probe_languages(sb, &languages).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "sandbox: language probe failed; no language is available");
                BTreeMap::new()
            }
        };
        let checks = languages
            .iter()
            .filter(|(id, _)| present.get(*id).copied().unwrap_or(false))
            .map(|(id, lang)| async move {
                let version = self_test(sb, lang, caps).await;
                (id.to_owned(), version)
            });
        let mut available = BTreeMap::new();
        for (id, result) in join_all(checks).await {
            match result {
                Ok(version) => {
                    available.insert(id, version);
                }
                Err(reason) => {
                    tracing::warn!(language = %id, %reason, "sandbox: language self-test failed")
                }
            }
        }
        for (id, _) in languages.iter() {
            if !present.get(id).copied().unwrap_or(false) {
                tracing::warn!(language = %id, "sandbox: language toolchain missing");
            }
        }
        Ok(Self {
            languages,
            available,
            caps,
        })
    }

    /// An available language.
    pub fn get(&self, id: &str) -> Option<&Language> {
        if self.available.contains_key(id) {
            self.languages.get(id)
        } else {
            None
        }
    }

    /// Every configured language and whether it passed its self-test, by id.
    pub fn catalogue(&self) -> Vec<LanguageStatus> {
        self.languages
            .iter()
            .map(|(id, _)| LanguageStatus {
                id: id.to_owned(),
                available: self.available.contains_key(id),
            })
            .collect()
    }

    /// The compiler `--version` probe output of an available language.
    pub fn toolchain_version(&self, id: &str) -> Option<&str> {
        self.available.get(id).map(String::as_str)
    }

    /// The full table, including unavailable languages (for env validation).
    pub fn languages(&self) -> &Languages {
        &self.languages
    }

    pub fn output_caps(&self) -> OutputCaps {
        self.caps
    }
}

/// Probes the version, then compiles and runs the hello-world program.
async fn self_test(sb: &dyn Sandbox, lang: &Language, caps: OutputCaps) -> Result<String, String> {
    let version = toolchain_version(sb, lang)
        .await
        .ok_or_else(|| "version probe failed".to_owned())?;
    let mut file_id = None;
    if let Some(cmd) = compile_cmd(lang, &lang.hello, &BTreeMap::new(), &[]) {
        let r = first(sb.run(vec![cmd]).await)?;
        let binary = lang.overlay.binary.as_deref().unwrap_or_default();
        match r.file_ids.get(binary) {
            Some(id) if r.status == STATUS_ACCEPTED => file_id = Some(crate::FileId(id.clone())),
            _ => {
                let stderr = r.files.get("stderr").map_or("", String::as_str);
                return Err(format!("compile: {} {}", r.status, stderr.trim()));
            }
        }
    }
    let cmd = run_cmd(
        lang,
        &RunInput {
            source: &lang.hello,
            file_id: file_id.as_ref(),
            ..RunInput::default()
        },
        caps,
    );
    let run = first(sb.run(vec![cmd]).await);
    if let Some(id) = file_id
        && let Err(e) = sb.delete_file(id).await
    {
        tracing::warn!(language = %lang.id, error = %e, "sandbox: release self-test binary");
    }
    let r = run?;
    let stdout = r.files.get("stdout").map_or("", String::as_str);
    if r.status != STATUS_ACCEPTED || stdout.trim() != "hello" {
        return Err(format!("run: {} {:?}", r.status, stdout));
    }
    Ok(version)
}

fn first(r: Result<Vec<crate::RunResult>, Error>) -> Result<crate::RunResult, String> {
    r.map_err(|e| e.to_string())?
        .into_iter()
        .next()
        .ok_or_else(|| "no result".to_owned())
}
