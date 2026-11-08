//! go-judge client against a wiremock server. Ports client_test.go and probe_test.go.

use ae_core::Config;
use ae_sandbox::{
    Cmd, CmdFile, FileId, GoJudge, Languages, STATUS_ACCEPTED, STATUS_NONZERO_EXIT, Sandbox,
    probe_languages, with_request_id,
};
use std::collections::HashMap;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn judge(url: &str, token: &str) -> GoJudge {
    let m: HashMap<String, String> = [("AC_JUDGE_URL", url), ("AC_JUDGE_TOKEN", token)]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
    GoJudge::new(&Config::from_map(&m).unwrap()).unwrap()
}

fn echo_cmd() -> Cmd {
    Cmd {
        args: vec!["/bin/echo".into(), "hi".into()],
        files: vec![
            CmdFile::stdin(""),
            CmdFile::collector("stdout", 10),
            CmdFile::collector("stderr", 10),
        ],
        cpu_limit: 1,
        memory_limit: 2,
        proc_limit: 3,
        ..Cmd::default()
    }
}

#[tokio::test]
async fn run_posts_cmds_with_bearer_token_and_decodes_results() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/run"))
        .and(header("authorization", "Bearer s3cret"))
        .and(header("content-type", "application/json"))
        .and(body_partial_json(serde_json::json!({"cmd": [{
            "args": ["/bin/echo", "hi"],
            "files": [{"content": ""}, {"name": "stdout", "max": 10}, {"name": "stderr", "max": 10}],
            "cpuLimit": 1, "memoryLimit": 2, "procLimit": 3
        }]})))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([{
            "status": "Accepted", "exitStatus": 0, "time": 5, "memory": 6, "runTime": 7,
            "files": {"stdout": "hi\n"}, "fileIds": {"/w/a.out": "F1"}
        }])))
        .expect(1)
        .mount(&server)
        .await;
    let res = judge(&server.uri(), "s3cret")
        .run(vec![echo_cmd()])
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].status, STATUS_ACCEPTED);
    assert_eq!(res[0].files["stdout"], "hi\n");
    assert_eq!(res[0].file_ids["/w/a.out"], "F1");
    assert_eq!((res[0].time, res[0].memory, res[0].run_time), (5, 6, 7));
}

#[tokio::test]
async fn run_omits_empty_optional_fields() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/run"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{"status":"Accepted"}])),
        )
        .mount(&server)
        .await;
    judge(&server.uri(), "t")
        .run(vec![echo_cmd()])
        .await
        .unwrap();
    let reqs = server.received_requests().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
    let cmd = body["cmd"][0].as_object().unwrap();
    for k in [
        "env",
        "clockLimit",
        "stackLimit",
        "copyIn",
        "copyOut",
        "copyOutCached",
    ] {
        assert!(!cmd.contains_key(k), "{k} sent");
    }
    assert!(!reqs[0].headers.contains_key("accept-encoding"));
}

#[tokio::test]
async fn run_reports_http_errors_and_count_mismatch() {
    let server = MockServer::start().await;
    Mock::given(path("/run"))
        .respond_with(ResponseTemplate::new(502).set_body_string("down"))
        .mount(&server)
        .await;
    let err = judge(&server.uri(), "tok-xyz")
        .run(vec![echo_cmd()])
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("502") && msg.contains("down"), "{msg}");
    assert!(!msg.contains("tok-xyz"));

    let server = MockServer::start().await;
    Mock::given(path("/run"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .mount(&server)
        .await;
    assert!(
        judge(&server.uri(), "t")
            .run(vec![echo_cmd()])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn version_reads_build_version() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .and(header("authorization", "Bearer t"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"buildVersion":"1.13.0"})),
        )
        .mount(&server)
        .await;
    assert_eq!(judge(&server.uri(), "t").version().await.unwrap(), "1.13.0");
}

struct CaptureId;
impl Respond for CaptureId {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let id = req
            .headers
            .get("x-request-id")
            .map(|v| v.to_str().unwrap().to_owned())
            .unwrap_or_default();
        if req.url.path() == "/version" {
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"buildVersion": id}))
        } else {
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!([{"status": "Accepted", "error": id}]))
        }
    }
}

#[tokio::test]
async fn client_propagates_request_id() {
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::any())
        .respond_with(CaptureId)
        .mount(&server)
        .await;
    let j = judge(&server.uri(), "t");
    let (run, ver) = with_request_id("test-req-id-1234".into(), async {
        (
            j.run(vec![echo_cmd()]).await.unwrap(),
            j.version().await.unwrap(),
        )
    })
    .await;
    assert_eq!(run[0].error, "test-req-id-1234");
    assert_eq!(ver, "test-req-id-1234");
    assert_eq!(j.run(vec![echo_cmd()]).await.unwrap()[0].error, "");
    assert_eq!(j.version().await.unwrap(), "");
}

#[tokio::test]
async fn cache_and_delete_file() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/file"))
        .and(header("authorization", "Bearer t"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!("ABC123")))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/file/ABC123"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let j = judge(&server.uri(), "t");
    let id = j.cache_file("main.py", b"print(1)").await.unwrap();
    assert_eq!(id, FileId("ABC123".into()));
    let body = &server.received_requests().await.unwrap()[0].body;
    assert!(String::from_utf8_lossy(body).contains("print(1)"));
    j.delete_file(id).await.unwrap();
}

#[tokio::test]
async fn delete_file_rejects_path_injection() {
    let server = MockServer::start().await;
    let j = judge(&server.uri(), "t");
    for bad in ["../run", "a/b", "a?x=1", "", "a b"] {
        assert!(j.delete_file(FileId(bad.into())).await.is_err(), "{bad}");
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[test]
fn debug_never_shows_token() {
    let j = judge("http://127.0.0.1:1", "very-secret-token");
    assert!(!format!("{j:?}").contains("very-secret-token"));
}

struct ProbeResponder;
impl Respond for ProbeResponder {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        let out: Vec<serde_json::Value> = body["cmd"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                assert_eq!(c["args"][0], "/usr/bin/test");
                assert_eq!(c["args"][1], "-x");
                let bin = c["args"][2].as_str().unwrap();
                if bin == "/usr/bin/rustc" || bin == "/usr/bin/php" {
                    serde_json::json!({"status": STATUS_NONZERO_EXIT, "exitStatus": 1})
                } else {
                    serde_json::json!({"status": STATUS_ACCEPTED})
                }
            })
            .collect();
        ResponseTemplate::new(200).set_body_json(out)
    }
}

#[test]
fn lang_binaries() {
    let l = Languages::builtin();
    assert_eq!(l.get("c").unwrap().binaries(), vec!["/usr/bin/gcc"]);
    assert_eq!(
        l.get("java").unwrap().binaries(),
        vec!["/usr/bin/javac", "/usr/bin/java"]
    );
    assert_eq!(
        l.get("python").unwrap().binaries(),
        vec!["/usr/bin/python3"]
    );
}

#[tokio::test]
async fn probe_languages_marks_missing_binaries() {
    let server = MockServer::start().await;
    Mock::given(path("/run"))
        .respond_with(ProbeResponder)
        .expect(1)
        .mount(&server)
        .await;
    let l = Languages::builtin();
    let avail = probe_languages(&judge(&server.uri(), "t"), &l)
        .await
        .unwrap();
    assert!(!avail["rust"] && !avail["php"]);
    assert!(avail["c"] && avail["java"] && avail["python"]);
    assert_eq!(avail.len(), l.iter().count());
}

#[tokio::test]
async fn probe_languages_reports_a_sandbox_failure() {
    let server = MockServer::start().await;
    Mock::given(path("/run"))
        .respond_with(ResponseTemplate::new(502))
        .mount(&server)
        .await;
    assert!(
        probe_languages(&judge(&server.uri(), "t"), &Languages::builtin())
            .await
            .is_err()
    );
}

#[test]
fn non_http_judge_url_is_a_config_error() {
    for url in ["https://judge:5050", "unix:///run/judge.sock", "judge:5050"] {
        let m: HashMap<String, String> = [("AC_JUDGE_URL".to_owned(), url.to_owned())].into();
        let err = GoJudge::new(&Config::from_map(&m).unwrap()).unwrap_err();
        assert!(matches!(err, ae_sandbox::Error::Config(_)), "{url}: {err}");
    }
}

#[test]
fn zero_collector_max_is_omitted_like_go() {
    let v = serde_json::to_value(CmdFile::collector("stdout", 0)).unwrap();
    assert_eq!(v, serde_json::json!({"name": "stdout"}));
    let v = serde_json::to_value(CmdFile::collector("stdout", 5)).unwrap();
    assert_eq!(v, serde_json::json!({"name": "stdout", "max": 5}));
}

#[tokio::test]
async fn run_piped_sends_pipe_mapping_and_run_omits_it() {
    use ae_sandbox::{PipeIndex, PipeMap};
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/run"))
        .and(body_partial_json(serde_json::json!({"pipeMapping": [
            {"in": {"index": 0, "fd": 1}, "out": {"index": 1, "fd": 0}, "proxy": true}
        ]})))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"status": "Accepted"}, {"status": "Accepted"}
        ])))
        .expect(1)
        .mount(&server)
        .await;
    let pipes = vec![PipeMap {
        input: PipeIndex { index: 0, fd: 1 },
        output: PipeIndex { index: 1, fd: 0 },
        proxy: true,
    }];
    let res = judge(&server.uri(), "t")
        .run_piped(vec![echo_cmd(), echo_cmd()], pipes)
        .await
        .unwrap();
    assert_eq!(res.len(), 2);

    let plain = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/run"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{"status":"Accepted"}])),
        )
        .mount(&plain)
        .await;
    judge(&plain.uri(), "t")
        .run(vec![echo_cmd()])
        .await
        .unwrap();
    let body: serde_json::Value =
        serde_json::from_slice(&plain.received_requests().await.unwrap()[0].body).unwrap();
    assert!(body.get("pipeMapping").is_none(), "{body}");
}

#[tokio::test]
async fn request_timeout_bounds_a_hung_judge() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/version"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"buildVersion": "v"}))
                .set_delay(std::time::Duration::from_secs(30)),
        )
        .mount(&server)
        .await;
    let m: HashMap<String, String> = [("AC_JUDGE_URL".to_owned(), server.uri())].into();
    let j = GoJudge::with_request_timeout(
        &Config::from_map(&m).unwrap(),
        std::time::Duration::from_millis(200),
    )
    .unwrap();
    let start = std::time::Instant::now();
    assert!(j.version().await.is_err());
    assert!(start.elapsed() < std::time::Duration::from_secs(5));
}
