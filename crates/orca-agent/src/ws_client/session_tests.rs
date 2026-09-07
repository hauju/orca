//! The session read loop must stay responsive while a handler does runtime
//! work: a `StatusPing` whose report walks a slow runtime must not delay the
//! receipt ACK of the `Deploy` queued behind it.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use orca_core::runtime::Runtime;
use orca_core::testing::MockRuntime;
use orca_core::types::{WorkloadSpec, WorkloadStatus};
use orca_core::ws_types::{AgentMessage, MasterMessage};

use super::handle_ws_session;
use crate::grpc::AgentClient;

type MasterSocket = WebSocketStream<TcpStream>;

async fn send(master: &mut MasterSocket, msg: MasterMessage) {
    let json = serde_json::to_string(&msg).expect("serializable");
    master
        .send(Message::Text(json.into()))
        .await
        .expect("send to agent");
}

async fn next_message(master: &mut MasterSocket) -> AgentMessage {
    loop {
        let frame = master.next().await.expect("open").expect("frame");
        if let Message::Text(text) = frame {
            return serde_json::from_str(&text).expect("agent message");
        }
    }
}

fn spec(name: &str) -> WorkloadSpec {
    serde_json::from_value(serde_json::json!({
        "name": name,
        "runtime": "container",
        "image": "nginx:alpine",
        "replicas": 1,
        "routes": [],
        "env": {},
        "aliases": [],
        "mounts": [],
        "triggers": [],
        "internal": false,
    }))
    .expect("valid workload spec")
}

/// The production sequence: a `StatusPing`, then a `Deploy` right behind it.
/// The receipt ACK must be the first frame on the wire; handling the ping
/// inline put a full report walk ahead of it, past the master's ACK window.
#[tokio::test]
async fn status_ping_does_not_delay_the_next_deploy_ack() {
    // Arrange: a loopback "master", one agent session, and a runtime whose
    // status call takes a second — one workload is enough to make an inline
    // report walk outlast the whole assertion window.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let accept = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        tokio_tungstenite::accept_async(stream)
            .await
            .expect("handshake")
    });
    let (client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
        .await
        .expect("connect");
    let mut master = accept.await.expect("join");

    let mut runtime = MockRuntime::new();
    runtime.status_delay = Duration::from_secs(1);
    let runtime: Arc<dyn Runtime> = Arc::new(runtime);
    let agent = Arc::new(AgentClient::new("http://127.0.0.1:1".into(), 1));
    agent
        .update_workload_status("mock-web", "web", WorkloadStatus::Running)
        .await;
    let (domain_tx, _domain_rx) = mpsc::channel(4);
    let session =
        tokio::spawn(
            async move { handle_ws_session(client, 1, &runtime, &agent, &domain_tx).await },
        );

    // Act.
    send(&mut master, MasterMessage::StatusPing).await;
    send(
        &mut master,
        MasterMessage::Deploy {
            spec: Box::new(spec("web")),
        },
    )
    .await;

    // Assert: the ACK arrives well before any report walk can finish.
    let first = tokio::time::timeout(Duration::from_millis(500), next_message(&mut master))
        .await
        .expect("agent answered within the ACK window");
    assert!(
        matches!(&first, AgentMessage::DeployReceived { service_name } if service_name == "web"),
        "expected DeployReceived first, got {first:?}"
    );
    session.abort();
}
