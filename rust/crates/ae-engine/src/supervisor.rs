//! Supervision of the background tasks that must run for the life of the process
//! (registry refresher, queue sampler, reaper, budget gauge, metrics server). Any of
//! them finishing, by returning, erroring or panicking, is a failure of the engine.

use std::collections::HashMap;
use std::future::Future;
use tokio::task::{Id, JoinHandle, JoinSet};

#[derive(Default)]
pub struct Supervisor {
    set: JoinSet<Result<(), String>>,
    names: HashMap<Id, &'static str>,
}

/// Aborts the adopted task when the supervising task is dropped or aborted.
struct Adopted(JoinHandle<()>);

impl Drop for Adopted {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Supervisor {
    /// Runs `fut`, which must never finish while the engine is up.
    pub fn spawn<F>(&mut self, name: &'static str, fut: F)
    where
        F: Future<Output = Result<(), String>> + Send + 'static,
    {
        let id = self.set.spawn(fut).id();
        self.names.insert(id, name);
    }

    /// Supervises a task spawned elsewhere; it is aborted with the supervisor.
    pub fn adopt(&mut self, name: &'static str, handle: JoinHandle<()>) {
        // Built before the future so it aborts the task even if never polled.
        let mut a = Adopted(handle);
        self.spawn(
            name,
            async move { (&mut a.0).await.map_err(|e| e.to_string()) },
        );
    }

    /// Waits for the first task to finish and describes it as `(component, reason)`.
    /// Pending forever when nothing is supervised.
    pub async fn failure(&mut self) -> (&'static str, String) {
        let Some(r) = self.set.join_next_with_id().await else {
            return std::future::pending().await;
        };
        let (id, why) = match r {
            Ok((id, Ok(()))) => (id, "exited".to_owned()),
            Ok((id, Err(e))) => (id, e),
            Err(e) => (e.id(), e.to_string()),
        };
        (self.names.get(&id).copied().unwrap_or("unknown"), why)
    }

    /// Aborts every task and waits for them to stop.
    pub async fn shutdown(mut self) {
        self.set.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn reports_the_component_that_exits_or_panics() {
        let mut s = Supervisor::default();
        s.spawn("forever", std::future::pending());
        s.spawn("quitter", async { Ok(()) });
        assert_eq!(s.failure().await, ("quitter", "exited".to_owned()));

        s.adopt("reaper", tokio::spawn(async { panic!("boom") }));
        let (name, why) = s.failure().await;
        assert_eq!(name, "reaper");
        assert!(why.contains("panic"), "{why}");

        s.spawn("server", async { Err("bind lost".to_owned()) });
        assert_eq!(s.failure().await, ("server", "bind lost".to_owned()));

        // Only the pending task is left.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), s.failure())
                .await
                .is_err()
        );
        s.shutdown().await;
    }

    #[tokio::test]
    async fn adopted_tasks_are_aborted_with_the_supervisor() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let mut s = Supervisor::default();
        s.adopt(
            "sampler",
            tokio::spawn(async move {
                let _keep = tx;
                std::future::pending::<()>().await;
            }),
        );
        s.shutdown().await;
        // The sender was dropped, so the adopted task is gone.
        assert!(rx.await.is_err());
    }
}
