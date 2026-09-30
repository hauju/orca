//! Workload status reporting and per-container stats collection for the agent.

use futures_util::stream::{self, StreamExt};
use tokio::io::AsyncReadExt;

use orca_core::runtime::{LogOpts, Runtime, WorkloadHandle};
use orca_core::types::WorkloadStatus;

use super::AgentClient;
use super::WorkloadInfo;

/// Max bytes of log tail to ship in a heartbeat for a crashed container.
const LOG_TAIL_LIMIT: usize = 4096;

/// Workloads queried at once when building a heartbeat. Each running
/// container costs ~0.75s on Docker (two stats samples 500ms apart); one at a
/// time, 45 containers took ~35s — past the master's 30s idle deadline, so it
/// dropped the node's control session on every heartbeat.
const REPORT_CONCURRENCY: usize = 16;

/// Fetch the last few log lines of a (typically crashed) container so the
/// master can explain the failure K8s-style. Returns `None` on error/empty.
async fn read_log_tail(runtime: &dyn Runtime, handle: &WorkloadHandle) -> Option<String> {
    let opts = LogOpts {
        follow: false,
        tail: Some(20),
        since: None,
        timestamps: false,
    };
    let mut stream = runtime.logs(handle, &opts).await.ok()?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.ok()?;
    let text = String::from_utf8_lossy(&buf);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Keep the most recent bytes if oversized, so the heartbeat stays small.
    let out = if trimmed.len() > LOG_TAIL_LIMIT {
        format!("…{}", &trimmed[trimmed.len() - LOG_TAIL_LIMIT..])
    } else {
        trimmed.to_string()
    };
    Some(out)
}

impl AgentClient {
    /// Collect workload status reports with per-container stats for heartbeat.
    pub async fn collect_workload_reports(
        &self,
        runtime: &dyn Runtime,
    ) -> Vec<orca_core::ws_types::WorkloadReport> {
        // Snapshot under the read lock, then query the runtime without
        // holding it (stats/log calls can be slow).
        let snapshot: Vec<(String, String, WorkloadStatus)> = self
            .workloads
            .read()
            .await
            .iter()
            .map(|(id, info)| (id.clone(), info.service_name.clone(), info.status))
            .collect();

        stream::iter(snapshot)
            .map(|(id, service_name, status)| async move {
                let handle = WorkloadHandle {
                    runtime_id: id.clone(),
                    name: format!("orca-{service_name}"),
                    metadata: Default::default(),
                };
                // Running: collect live stats. Not running: collect crash detail
                // (exit code, restart count, log tail) so the master can explain
                // *why* without a separate fetch.
                let (cpu, mem, exit_code, restart_count, last_logs) =
                    if status == WorkloadStatus::Running {
                        let (cpu, mem) = match runtime.stats(&handle).await {
                            Ok(rs) => (rs.cpu_percent, rs.memory_bytes),
                            Err(_) => (0.0, 0),
                        };
                        (cpu, mem, None, 0, None)
                    } else {
                        let exit = runtime.last_exit(&handle).await.unwrap_or_default();
                        let logs = read_log_tail(runtime, &handle).await;
                        (0.0, 0, exit.exit_code, exit.restart_count, logs)
                    };

                orca_core::ws_types::WorkloadReport {
                    service_name,
                    status: match status {
                        WorkloadStatus::Running => "running",
                        WorkloadStatus::Stopped | WorkloadStatus::Completed => "stopped",
                        WorkloadStatus::Failed => "failed",
                        WorkloadStatus::Pending | WorkloadStatus::Creating => "pending",
                        WorkloadStatus::Stopping => "stopping",
                    }
                    .into(),
                    container_id: Some(id),
                    cpu_percent: cpu,
                    memory_bytes: mem,
                    exit_code,
                    restart_count,
                    last_logs,
                }
            })
            .buffered(REPORT_CONCURRENCY)
            .collect()
            .await
    }

    pub async fn update_workload_status(&self, id: &str, service: &str, status: WorkloadStatus) {
        let mut workloads = self.workloads.write().await;
        workloads.insert(
            id.to_string(),
            WorkloadInfo {
                service_name: service.to_string(),
                status,
            },
        );
    }
}
