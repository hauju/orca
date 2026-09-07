//! Tests for the two-phase remote-deploy timeout (#88 / #94).
//!
//! The master waits for a short *receipt* ACK (agent got the command) and then
//! a long *completion* ACK (deploy finished). This ensures:
//! - an unreachable agent fails fast with a distinct message, and
//! - a real agent-side error (e.g. image-not-found) surfaces verbatim instead
//!   of being masked by a bare 30s timeout.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::sync::mpsc;

use orca_control::state::{AppState, RegisteredNode};
use orca_core::config::{ClusterConfig, ServiceConfig};
use orca_core::testing::MockRuntime;
use orca_core::types::{PlacementConstraint, Replicas, RuntimeKind};
use orca_core::ws_types::MasterMessage;

fn make_state(cfg: ClusterConfig) -> Arc<AppState> {
    let runtime = Arc::new(MockRuntime::new());
    Arc::new(AppState::new(
        cfg,
        runtime,
        None,
        Arc::new(RwLock::new(HashMap::new())),
        Arc::new(RwLock::new(Vec::new())),
    ))
}

fn config_placed_on(name: &str, node: &str) -> ServiceConfig {
    ServiceConfig {
        restart_policy: None,
        name: name.into(),
        project: None,
        runtime: RuntimeKind::Container,
        image: Some("nginx:latest".into()),
        module: None,
        replicas: Replicas::Fixed(1),
        port: Some(8080),
        host_port: None,
        domain: None,
        domains: vec![],
        routes: vec![],
        health: None,
        readiness: None,
        liveness: None,
        env: HashMap::new(),
        resources: None,
        volume: None,
        deploy: None,
        placement: Some(PlacementConstraint {
            labels: None,
            node: Some(node.into()),
            requires_gpu: None,
        }),
        network: None,
        aliases: vec![],
        mounts: vec![],
        triggers: vec![],
        assets: None,
        build: None,
        tls_cert: None,
        tls_key: None,
        internal: false,
        depends_on: vec![],
        cmd: vec![],
        extra_ports: vec![],
        strip_prefix: None,
        pull_policy: Default::default(),
        backup: None,
    }
}

async fn register_node(state: &AppState, node_id: u64) {
    state.registered_nodes.write().await.insert(
        node_id,
        RegisteredNode {
            peer_ip: None,
            node_id,
            address: format!("node-{node_id}:6881"),
            labels: HashMap::new(),
            last_heartbeat: chrono::Utc::now(),
            drain: false,
            cpu_percent: 0.0,
            memory_bytes: 0,
            memory_total: 0,
            disk_used: 0,
            disk_total: 0,
            net_rx: 0,
            net_tx: 0,
        },
    );
}

/// Backdate a node's heartbeat past the read-idle deadline: an agent that has
/// gone silent, as opposed to one that is merely busy.
async fn silence_node(state: &AppState, node_id: u64) {
    let idle = state.cluster_config.deploy.ws_idle_timeout_secs as i64;
    if let Some(node) = state.registered_nodes.write().await.get_mut(&node_id) {
        node.last_heartbeat = chrono::Utc::now() - chrono::Duration::seconds(idle + 1);
    }
}

/// Install a WS session for a node and return the master->agent receiver.
async fn connect_session(state: &AppState, node_id: u64) -> mpsc::Receiver<MasterMessage> {
    let (tx, rx) = mpsc::channel::<MasterMessage>(8);
    state
        .ws_agents
        .write()
        .await
        .insert(node_id, orca_control::state::AgentSession::new(tx));
    rx
}

/// When a silent agent never acknowledges receipt, the deploy must fail with
/// a distinct "did not acknowledge / unreachable" message after the short ACK
/// window — NOT the old opaque "timed out after 30 s".
#[tokio::test]
async fn ack_timeout_reports_unreachable_distinctly() {
    let mut cfg = ClusterConfig::default();
    cfg.deploy.ack_timeout_secs = 1; // keep the test fast
    let state = make_state(cfg);
    register_node(&state, 1).await;
    silence_node(&state, 1).await;

    // Hold the receiver open, so the Deploy send succeeds but nothing ever
    // replies with DeployReceived.
    let _rx = connect_session(&state, 1).await;

    let cfg = config_placed_on("svc", "1");
    let (deployed, errors) = orca_control::reconciler::reconcile(&state, &[cfg]).await;

    assert!(deployed.is_empty(), "deploy should not be recorded");
    assert_eq!(errors.len(), 1, "expected one error, got {errors:?}");
    let err = &errors[0];
    assert!(
        err.contains("did not acknowledge") && err.contains("unreachable"),
        "expected a distinct unreachable message, got: {err}"
    );
    assert!(
        !err.contains("timed out after 30"),
        "must not emit the old opaque 30s timeout: {err}"
    );

    // Both waiter maps must be cleaned up so they don't leak.
    assert!(state.pending_deploy_acks.read().await.is_empty());
    assert!(state.pending_deploys.read().await.is_empty());

    // #131: a missed ACK is proof the control session is dead — it must be
    // torn down so the node stops looking reachable and the next deploy
    // fails fast with the real story instead of re-timing-out forever.
    assert!(
        state.ws_agents.read().await.is_empty(),
        "dead control session must be killed after ACK timeout"
    );
    // And the node's remote placeholders must be gone, so status stops
    // reporting last-known state as current.
    let services = state.services.read().await;
    if let Some(svc) = services.get("svc") {
        assert!(
            svc.instances.is_empty(),
            "remote placeholders must be removed with the dead session"
        );
    }
}

/// An agent that misses the ACK window but is still heartbeating is busy, not
/// gone: its session must survive, the master must keep waiting, and the
/// deploy must succeed once the agent catches up. This was the production
/// failure — a slow inline handler on the agent delayed the ACK by ~18s while
/// heartbeats kept flowing, and the master killed the session and reported a
/// deploy as failed that then completed.
#[tokio::test]
async fn heartbeating_agent_is_waited_for_not_killed() {
    let mut cfg = ClusterConfig::default();
    cfg.deploy.ack_timeout_secs = 1;
    let state = make_state(cfg);
    register_node(&state, 1).await; // heartbeat: now
    let mut rx = connect_session(&state, 1).await;

    let cfg = config_placed_on("svc", "1");
    let state_c = state.clone();
    let deploy =
        tokio::spawn(async move { orca_control::reconciler::reconcile(&state_c, &[cfg]).await });
    let msg = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("master should send a Deploy")
        .expect("channel open");
    assert!(matches!(msg, MasterMessage::Deploy { .. }));

    // Two ACK windows pass without an ACK.
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    assert!(
        state.ws_agents.read().await.contains_key(&1),
        "a heartbeating agent's session must not be killed on a missed ACK"
    );
    assert!(
        state.pending_deploy_acks.read().await.contains_key("svc"),
        "the master must still be waiting for the ACK"
    );

    // The agent catches up: receipt, then a successful result.
    state
        .pending_deploy_acks
        .write()
        .await
        .remove("svc")
        .expect("ack waiter registered")
        .send(())
        .ok();
    state
        .pending_deploys
        .write()
        .await
        .remove("svc")
        .expect("result waiter registered")
        .send(Ok(()))
        .ok();

    let (deployed, errors) = deploy.await.unwrap();
    assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    assert!(
        deployed.contains(&"svc".to_string()),
        "the late-acknowledged deploy must count as deployed"
    );
}

/// The wait for a busy agent is bounded by the completion budget, and running
/// out of it is a timeout on a live session — not grounds to kill it.
#[tokio::test]
async fn heartbeating_agent_that_never_acks_times_out_without_a_kill() {
    let mut cfg = ClusterConfig::default();
    cfg.deploy.ack_timeout_secs = 1;
    cfg.deploy.completion_timeout_secs = 2;
    let state = make_state(cfg);
    register_node(&state, 1).await;
    let _rx = connect_session(&state, 1).await;

    let cfg = config_placed_on("svc", "1");
    let (deployed, errors) = orca_control::reconciler::reconcile(&state, &[cfg]).await;

    assert!(deployed.is_empty());
    assert_eq!(errors.len(), 1, "expected one error, got {errors:?}");
    let err = &errors[0];
    assert!(
        err.contains("heartbeating") && err.contains("timed out"),
        "expected a live-but-unresponsive message, got: {err}"
    );
    assert!(
        !err.contains("unreachable"),
        "a heartbeating agent must not be reported unreachable: {err}"
    );
    assert!(
        state.ws_agents.read().await.contains_key(&1),
        "the live session must survive the timeout"
    );
    assert!(state.pending_deploy_acks.read().await.is_empty());
    assert!(state.pending_deploys.read().await.is_empty());
}

/// Once the agent acknowledges receipt, a real deploy failure (e.g. image not
/// found) must surface verbatim instead of being masked by a timeout.
#[tokio::test]
async fn real_agent_error_propagates_after_ack() {
    let state = make_state(ClusterConfig::default()); // default 10s/600s timeouts
    register_node(&state, 1).await;
    let mut rx = connect_session(&state, 1).await;

    let cfg = config_placed_on("svc", "1");
    let state_c = state.clone();
    let deploy =
        tokio::spawn(async move { orca_control::reconciler::reconcile(&state_c, &[cfg]).await });

    // The master pushes the Deploy command; receiving it confirms both waiter
    // entries are registered.
    let msg = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("master should send a Deploy")
        .expect("channel open");
    assert!(matches!(msg, MasterMessage::Deploy { .. }));

    // Simulate the agent: acknowledge receipt, then report a pull failure.
    state
        .pending_deploy_acks
        .write()
        .await
        .remove("svc")
        .expect("ack waiter registered")
        .send(())
        .ok();
    state
        .pending_deploys
        .write()
        .await
        .remove("svc")
        .expect("result waiter registered")
        .send(Err(
            "pull access denied for ghcr.io/x:nope, not found".into()
        ))
        .ok();

    let (deployed, errors) = deploy.await.unwrap();
    assert!(deployed.is_empty());
    assert_eq!(errors.len(), 1, "expected one error, got {errors:?}");
    let err = &errors[0];
    assert!(
        err.contains("not found") && err.contains("ghcr.io/x:nope"),
        "real pull error must propagate verbatim, got: {err}"
    );
    assert!(
        !err.contains("timed out") && !err.contains("did not"),
        "should not be a timeout once the agent reported a result: {err}"
    );
}

/// #120 regression: reconciling an UNCHANGED placement-pinned service must
/// not dispatch a deploy. The remote branch lacked the local branch's
/// same-spec skip, so full-tree reconciles (the infra webhook) force-
/// recreated every remote service across unrelated projects.
#[tokio::test]
async fn unchanged_remote_spec_is_not_redispatched() {
    let state = make_state(ClusterConfig::default());
    register_node(&state, 1).await;
    let mut rx = connect_session(&state, 1).await;

    let cfg = config_placed_on("svc", "1");

    // Simulate a service already deployed to node 1: stored config matches
    // and the remote placeholder is Running.
    {
        let mut services = state.services.write().await;
        let mut svc = orca_control::state::ServiceState::from_config(cfg.clone());
        svc.instances.push(orca_control::state::InstanceState {
            handle: orca_core::runtime::WorkloadHandle {
                runtime_id: "remote-1".into(),
                name: "orca-svc".into(),
                metadata: Default::default(),
            },
            status: orca_core::types::WorkloadStatus::Running,
            host_port: None,
            container_address: None,
            health: orca_core::types::HealthState::NoCheck,
            started_at: std::time::Instant::now(),
            is_canary: false,
        });
        services.insert("svc".into(), svc);
    }

    let (deployed, errors) = orca_control::reconciler::reconcile(&state, &[cfg.clone()]).await;
    assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    assert!(
        deployed.contains(&"svc".to_string()),
        "skip still counts as converged"
    );
    assert!(
        rx.try_recv().is_err(),
        "unchanged spec must not dispatch a Deploy to the agent"
    );

    // A CHANGED spec must still dispatch.
    let mut changed = cfg;
    changed.env.insert("NEW_VAR".into(), "value".into());
    let state_c = state.clone();
    let handle =
        tokio::spawn(
            async move { orca_control::reconciler::reconcile(&state_c, &[changed]).await },
        );
    let msg = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
        .await
        .expect("changed spec must dispatch")
        .expect("channel open");
    assert!(
        matches!(msg, MasterMessage::Deploy { .. }),
        "expected Deploy, got {msg:?}"
    );
    // No agent to answer, and the node's heartbeat is fresh, so the master
    // would wait out the completion budget: drop the receipt waiter to end
    // the deploy now. Its outcome is not under test here.
    drop(rx);
    state.pending_deploy_acks.write().await.remove("svc");
    let _ = handle.await;
}
