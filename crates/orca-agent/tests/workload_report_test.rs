//! Heartbeat reports must be cheap to build even on a node with many
//! containers.

use std::time::Duration;

use orca_agent::grpc::AgentClient;
use orca_core::testing::MockRuntime;
use orca_core::types::WorkloadStatus;

/// Stats must be gathered concurrently. Sequentially, each running container
/// costs ~0.75s on Docker, so 45 of them took ~35s per heartbeat — past the
/// master's 30s idle deadline — and the node was dropped every cycle.
#[tokio::test]
async fn report_collects_stats_concurrently() {
    let mut runtime = MockRuntime::new();
    runtime.stats_delay = Some(Duration::from_millis(100));
    let agent = AgentClient::new("http://127.0.0.1:1".into(), 1);
    for i in 0..20 {
        agent
            .update_workload_status(
                &format!("mock-{i}"),
                &format!("svc-{i}"),
                WorkloadStatus::Running,
            )
            .await;
    }

    let started = std::time::Instant::now();
    let reports = agent.collect_workload_reports(&runtime).await;
    let elapsed = started.elapsed();

    assert_eq!(reports.len(), 20);
    assert!(
        elapsed < Duration::from_secs(1),
        "20 × 100ms of stats took {elapsed:?} — collected one at a time"
    );
}
