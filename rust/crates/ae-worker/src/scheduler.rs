//! Capacity scheduler: jobs are admitted against a node budget of CPU and
//! memory instead of a fixed worker count, first come first served.

use ae_core::Config;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::oneshot;

/// What a job holds while it runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Cost {
    /// Thousandths of a core.
    pub cpu_millis: u64,
    pub mem_bytes: u64,
}

impl Cost {
    fn fits(self, free: Cost) -> bool {
        self.cpu_millis <= free.cpu_millis && self.mem_bytes <= free.mem_bytes
    }
    fn min(self, o: Cost) -> Cost {
        Cost {
            cpu_millis: self.cpu_millis.min(o.cpu_millis),
            mem_bytes: self.mem_bytes.min(o.mem_bytes),
        }
    }
}

struct Waiter {
    cost: Cost,
    tx: oneshot::Sender<Permit>,
}

struct State {
    free: Cost,
    waiters: VecDeque<Waiter>,
}

struct Shared {
    budget: Cost,
    state: Mutex<State>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Cheap to clone; clones share the budget.
#[derive(Clone)]
pub struct Scheduler {
    shared: Arc<Shared>,
}

/// Holds a job's cost; returns it to the budget when dropped.
pub struct Permit {
    shared: Arc<Shared>,
    cost: Cost,
}

impl Drop for Permit {
    fn drop(&mut self) {
        if self.cost == Cost::default() {
            return;
        }
        let mut st = self.shared.lock();
        st.free.cpu_millis += self.cost.cpu_millis;
        st.free.mem_bytes = st.free.mem_bytes.saturating_add(self.cost.mem_bytes);
        grant(&self.shared, &mut st);
    }
}

/// Admits waiters from the front of the queue while they fit.
fn grant(shared: &Arc<Shared>, st: &mut State) {
    while let Some(front) = st.waiters.front() {
        if front.tx.is_closed() {
            st.waiters.pop_front();
            continue;
        }
        if !front.cost.fits(st.free) {
            return;
        }
        let Some(w) = st.waiters.pop_front() else {
            return;
        };
        st.free.cpu_millis -= w.cost.cpu_millis;
        st.free.mem_bytes -= w.cost.mem_bytes;
        let permit = Permit {
            shared: shared.clone(),
            cost: w.cost,
        };
        if let Err(mut p) = w.tx.send(permit) {
            // The waiter left; refund here (the lock is held, so not via Drop).
            st.free.cpu_millis += p.cost.cpu_millis;
            st.free.mem_bytes += p.cost.mem_bytes;
            p.cost = Cost::default();
        }
    }
}

impl Scheduler {
    pub fn new(budget: Cost) -> Self {
        Self {
            shared: Arc::new(Shared {
                budget,
                state: Mutex::new(State {
                    free: budget,
                    waiters: VecDeque::new(),
                }),
            }),
        }
    }

    /// One core per worker slot (`AC_WORKERS`, or auto when 0) and the node's
    /// memory (the cgroup limit when lower than physical RAM).
    pub fn budget_for(cfg: &Config) -> Cost {
        let workers = u64::try_from(cfg.effective_workers()).unwrap_or(u64::MAX);
        Cost {
            cpu_millis: workers.saturating_mul(1_000),
            mem_bytes: node_memory().unwrap_or(u64::MAX),
        }
    }

    pub fn budget(&self) -> Cost {
        self.shared.budget
    }

    pub fn available(&self) -> Cost {
        self.shared.lock().free
    }

    /// Waits until `cost` fits and every earlier waiter has been admitted. A
    /// cost larger than the whole budget is clamped to it, so the job runs alone.
    pub async fn acquire(&self, cost: Cost) -> Permit {
        let cost = cost.min(self.shared.budget);
        let rx = {
            let mut st = self.shared.lock();
            if st.waiters.is_empty() && cost.fits(st.free) {
                st.free.cpu_millis -= cost.cpu_millis;
                st.free.mem_bytes -= cost.mem_bytes;
                return Permit {
                    shared: self.shared.clone(),
                    cost,
                };
            }
            let (tx, rx) = oneshot::channel();
            st.waiters.push_back(Waiter { cost, tx });
            rx
        };
        match rx.await {
            Ok(p) => p,
            // The sender lives in our own queue, which outlives this call.
            Err(_) => Permit {
                shared: self.shared.clone(),
                cost: Cost::default(),
            },
        }
    }
}

/// `MemTotal`, lowered to the cgroup v2 `memory.max` when that is set. `None`
/// off Linux.
fn node_memory() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let total = meminfo
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))?
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse::<u64>()
        .ok()?
        .saturating_mul(1024);
    let cgroup = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok());
    Some(cgroup.map_or(total, |c| c.min(total)))
}
