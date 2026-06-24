#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const DEFAULT_URL: &str = "ws://zwave-api.localdomain/";
const READ_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Parser)]
#[command(name = "zwave")]
#[command(about = "Z-Wave JS Server CLI")]
struct Cli {
    /// Z-Wave JS Server websocket URL
    #[arg(short = 'u', long, env = "ZWAVE_WS_URL", default_value = DEFAULT_URL, global = true)]
    url: String,

    /// Print raw JSON
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show controller status and network summary
    Status,
    /// List nodes
    Nodes {
        /// Include only dead/not-ready nodes
        #[arg(long)]
        dead: bool,
    },
    /// List dead/not-ready nodes
    Dead,
    /// Ping a node
    Ping { node_id: u32 },
    /// Ask controller whether a node is failed
    IsFailed { node_id: u32 },
    /// Remove a node after the controller confirms it is failed
    RemoveFailed { node_id: u32 },
    /// Start route rebuilding
    RebuildRoutes,
    /// Start classic inclusion and print controller events
    Include {
        /// Seconds to wait before stopping inclusion
        #[arg(long, default_value_t = 60)]
        timeout: u64,
    },
    /// Call a Z-Wave JS Server command with a JSON payload
    Call {
        /// Command name, for example controller.get_node_neighbors
        command: String,
        /// JSON object payload
        #[arg(long, default_value = "{}")]
        payload: String,
    },
}

struct ZwaveClient {
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    next_id: u64,
}

struct NetworkSummary {
    ready_nodes: usize,
    status_counts: BTreeMap<String, usize>,
}

#[tokio::main]
#[cfg_attr(coverage_nightly, coverage(off))]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut client = ZwaveClient::connect(&cli.url).await?;

    match cli.command {
        Commands::Status => cmd_status(&mut client, cli.json).await,
        Commands::Nodes { dead } => cmd_nodes(&mut client, cli.json, dead).await,
        Commands::Dead => cmd_nodes(&mut client, cli.json, true).await,
        Commands::Ping { node_id } => {
            cmd_simple_node(&mut client, cli.json, "node.ping", node_id).await
        }
        Commands::IsFailed { node_id } => {
            cmd_simple_controller(&mut client, cli.json, "controller.is_failed_node", node_id).await
        }
        Commands::RemoveFailed { node_id } => {
            cmd_remove_failed(&mut client, cli.json, node_id).await
        }
        Commands::RebuildRoutes => cmd_rebuild_routes(&mut client, cli.json).await,
        Commands::Include { timeout } => cmd_include(&mut client, cli.json, timeout).await,
        Commands::Call { command, payload } => {
            cmd_call(&mut client, cli.json, &command, &payload).await
        }
    }
}

impl ZwaveClient {
    #[cfg_attr(coverage_nightly, coverage(off))]
    async fn connect(url: &str) -> Result<Self> {
        let (mut socket, _) = connect_async(url)
            .await
            .with_context(|| format!("connect to {url}"))?;

        let version = read_json(&mut socket).await?;
        let max_schema = version
            .get("maxSchemaVersion")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("server did not send maxSchemaVersion: {version}"))?;

        socket
            .send(Message::Text(
                json!({
                    "command": "set_api_schema",
                    "messageId": "schema",
                    "schemaVersion": max_schema,
                })
                .to_string()
                .into(),
            ))
            .await?;

        let schema = wait_for_message(&mut socket, "schema").await?;
        ensure_success(&schema)?;

        Ok(Self { socket, next_id: 1 })
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    async fn command(&mut self, command: &str, mut payload: Value) -> Result<Value> {
        let message_id = format!("m{}", self.next_id);
        self.next_id += 1;

        let object = payload
            .as_object_mut()
            .ok_or_else(|| anyhow!("payload must be a JSON object"))?;
        object.insert("command".to_string(), Value::String(command.to_string()));
        object.insert("messageId".to_string(), Value::String(message_id.clone()));

        self.socket
            .send(Message::Text(payload.to_string().into()))
            .await
            .with_context(|| format!("send {command}"))?;

        let result = wait_for_message(&mut self.socket, &message_id)
            .await
            .with_context(|| format!("wait for {command}"))?;
        ensure_success(&result)?;
        Ok(result)
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn cmd_status(client: &mut ZwaveClient, raw_json: bool) -> Result<()> {
    let state = start_listening(client).await?;
    if raw_json {
        println!("{}", serde_json::to_string_pretty(&state)?);
        return Ok(());
    }

    let controller = state
        .pointer("/result/state/controller")
        .unwrap_or(&Value::Null);
    let nodes = state
        .pointer("/result/state/nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("state did not contain nodes"))?;

    let summary = summarize_network(nodes);

    let stats = controller.get("statistics").unwrap_or(&Value::Null);
    println!(
        "Controller {} home={} nodes={} ready={} rebuilding={}",
        controller_firmware(controller),
        controller_home_id(controller),
        nodes.len(),
        summary.ready_nodes,
        value_bool(controller, "isRebuildingRoutes"),
    );
    println!(
        "TX={} RX={} dropped_tx={} dropped_rx={} timeouts={}",
        value_u64(stats, "messagesTX"),
        value_u64(stats, "messagesRX"),
        value_u64(stats, "messagesDroppedTX"),
        value_u64(stats, "messagesDroppedRX"),
        value_u64(stats, "timeoutResponse"),
    );
    println!("Status: {}", format_counts(&summary.status_counts));
    Ok(())
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn cmd_nodes(client: &mut ZwaveClient, raw_json: bool, dead_only: bool) -> Result<()> {
    let state = start_listening(client).await?;
    let nodes = state
        .pointer("/result/state/nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("state did not contain nodes"))?;

    let filtered: Vec<&Value> = nodes
        .iter()
        .filter(|node| !dead_only || !value_bool(node, "ready") || value_u64(node, "status") == 3)
        .collect();

    if raw_json {
        println!("{}", serde_json::to_string_pretty(&filtered)?);
        return Ok(());
    }

    println!(
        "{:<5} {:<8} {:<7} {:<9} {:<8} {}",
        "NODE", "STATUS", "READY", "LISTENING", "SLEEP", "LABEL"
    );
    for node in filtered {
        println!(
            "{:<5} {:<8} {:<7} {:<9} {:<8} {}",
            node_id(node),
            node.get("status")
                .map(status_name)
                .unwrap_or_else(|| "unknown".to_string()),
            value_bool(node, "ready"),
            value_bool(node, "isListening"),
            value_bool(node, "canSleep"),
            node_label(node),
        );
    }
    Ok(())
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn cmd_simple_node(
    client: &mut ZwaveClient,
    raw_json: bool,
    command: &str,
    node_id: u32,
) -> Result<()> {
    let result = client
        .command(command, json!({ "nodeId": node_id }))
        .await?;
    print_result(raw_json, &result)
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn cmd_simple_controller(
    client: &mut ZwaveClient,
    raw_json: bool,
    command: &str,
    node_id: u32,
) -> Result<()> {
    let result = client
        .command(command, json!({ "nodeId": node_id }))
        .await?;
    print_result(raw_json, &result)
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn cmd_remove_failed(client: &mut ZwaveClient, raw_json: bool, node_id: u32) -> Result<()> {
    let failed = client
        .command("controller.is_failed_node", json!({ "nodeId": node_id }))
        .await?;
    if !is_failed_node_result(&failed) {
        bail!("node {node_id} is not marked failed by the controller");
    }

    let result = client
        .command(
            "controller.remove_failed_node",
            json!({ "nodeId": node_id }),
        )
        .await?;
    print_result(raw_json, &result)
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn cmd_rebuild_routes(client: &mut ZwaveClient, raw_json: bool) -> Result<()> {
    let result = client
        .command("controller.begin_rebuilding_routes", json!({}))
        .await?;
    print_result(raw_json, &result)
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn cmd_include(client: &mut ZwaveClient, raw_json: bool, timeout: u64) -> Result<()> {
    let result = client
        .command("controller.begin_inclusion", json!({}))
        .await
        .context("start inclusion")?;
    print_result(raw_json, &result)?;
    eprintln!("Inclusion started; listening for {timeout}s...");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout);
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }

        match tokio::time::timeout_at(deadline, read_json(&mut client.socket)).await {
            Ok(Ok(value)) => print_inclusion_event(raw_json, &value)?,
            Ok(Err(err)) => return Err(err).context("read inclusion event"),
            Err(_) => break,
        }
    }

    match client.command("controller.stop_inclusion", json!({})).await {
        Ok(result) => {
            eprintln!("Inclusion stopped.");
            print_result(raw_json, &result)
        }
        Err(err) => Err(err).context("stop inclusion"),
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn cmd_call(
    client: &mut ZwaveClient,
    raw_json: bool,
    command: &str,
    payload: &str,
) -> Result<()> {
    let payload = parse_call_payload(payload)?;
    let result = client.command(command, payload).await?;
    print_result(raw_json, &result)
}

fn parse_call_payload(payload: &str) -> Result<Value> {
    let value: Value = serde_json::from_str(payload).context("parse payload JSON")?;
    if !value.is_object() {
        bail!("payload must be a JSON object");
    }
    Ok(value)
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn start_listening(client: &mut ZwaveClient) -> Result<Value> {
    client.command("start_listening", json!({})).await
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn print_result(raw_json: bool, result: &Value) -> Result<()> {
    if raw_json {
        println!("{}", serde_json::to_string_pretty(result)?);
    } else if let Some(value) = result.get("result") {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        println!("OK");
    }
    Ok(())
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn print_inclusion_event(raw_json: bool, value: &Value) -> Result<()> {
    if raw_json {
        println!("{}", serde_json::to_string_pretty(value)?);
        return Ok(());
    }

    println!("{}", format_inclusion_event(value));
    Ok(())
}

fn format_inclusion_event(value: &Value) -> String {
    let event = value
        .get("event")
        .or_else(|| value.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("message");
    let node_id = value
        .pointer("/node/nodeId")
        .or_else(|| value.get("nodeId"))
        .and_then(Value::as_u64);
    let status = value
        .get("status")
        .or_else(|| value.get("result"))
        .and_then(Value::as_str);

    match (node_id, status) {
        (Some(node_id), Some(status)) => format!("{event}: node={node_id} status={status}"),
        (Some(node_id), None) => format!("{event}: node={node_id}"),
        (None, Some(status)) => format!("{event}: status={status}"),
        (None, None) => format!("{event}: {value}"),
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn read_json(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Result<Value> {
    loop {
        let message = tokio::time::timeout(READ_TIMEOUT, socket.next())
            .await
            .context("websocket read timed out")?
            .ok_or_else(|| anyhow!("websocket closed"))??;

        match message {
            Message::Text(text) => return Ok(serde_json::from_str(&text)?),
            Message::Binary(bytes) => return Ok(serde_json::from_slice(&bytes)?),
            Message::Close(frame) => bail!("websocket closed: {frame:?}"),
            _ => continue,
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn wait_for_message(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    message_id: &str,
) -> Result<Value> {
    loop {
        let value = read_json(socket).await?;
        if value.get("messageId").and_then(Value::as_str) == Some(message_id) {
            return Ok(value);
        }
    }
}

fn ensure_success(value: &Value) -> Result<()> {
    if value.get("success").and_then(Value::as_bool) == Some(true) {
        return Ok(());
    }
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("unknown API error");
    bail!("{message}");
}

fn value_bool(value: &Value, key: &str) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn value_u64(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn is_failed_node_result(value: &Value) -> bool {
    value.pointer("/result/failed").and_then(Value::as_bool) == Some(true)
        || value.get("result").and_then(Value::as_bool) == Some(true)
}

fn summarize_network(nodes: &[Value]) -> NetworkSummary {
    let mut status_counts = BTreeMap::<String, usize>::new();
    let mut ready_nodes = 0usize;

    for node in nodes {
        if value_bool(node, "ready") {
            ready_nodes += 1;
        }

        let status = node
            .get("status")
            .map(status_name)
            .unwrap_or_else(|| "unknown".to_string());
        *status_counts.entry(status).or_default() += 1;
    }

    NetworkSummary {
        ready_nodes,
        status_counts,
    }
}

fn controller_firmware(controller: &Value) -> &str {
    controller
        .get("firmwareVersion")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
}

fn controller_home_id(controller: &Value) -> String {
    controller
        .get("homeId")
        .and_then(Value::as_u64)
        .map(|id| format!("{id:08x}"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn node_id(node: &Value) -> u64 {
    value_u64(node, "nodeId")
}

fn status_name(value: &Value) -> String {
    match value.as_u64() {
        Some(0) => "unknown".to_string(),
        Some(1) => "asleep".to_string(),
        Some(2) => "awake".to_string(),
        Some(3) => "dead".to_string(),
        Some(4) => "alive".to_string(),
        Some(other) => other.to_string(),
        None => "unknown".to_string(),
    }
}

fn node_label(node: &Value) -> String {
    let name = node.get("name").and_then(Value::as_str);
    let device_label = node.pointer("/deviceConfig/label").and_then(Value::as_str);
    let manufacturer = node
        .pointer("/deviceConfig/manufacturer")
        .and_then(Value::as_str);

    match (name, manufacturer, device_label) {
        (Some(name), Some(manufacturer), Some(label)) => {
            format!("{name} ({manufacturer} {label})")
        }
        (Some(name), _, _) => name.to_string(),
        (_, Some(manufacturer), Some(label)) => format!("{manufacturer} {label}"),
        (_, _, Some(label)) => label.to_string(),
        _ => "-".to_string(),
    }
}

fn format_counts(counts: &BTreeMap<String, usize>) -> String {
    counts
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn parse_call_payload_accepts_json_objects_only() {
        assert_eq!(
            parse_call_payload(r#"{"nodeId": 7}"#).expect("object"),
            json!({"nodeId": 7})
        );
        assert!(parse_call_payload("[1,2]").is_err());
        assert!(parse_call_payload("not json").is_err());
    }

    #[test]
    fn ensure_success_accepts_true_and_reports_message() {
        assert!(ensure_success(&json!({"success": true})).is_ok());

        let err = ensure_success(&json!({"success": false, "message": "bad"}))
            .expect_err("failure")
            .to_string();
        assert_eq!(err, "bad");
    }

    #[test]
    fn value_helpers_default_missing_or_wrong_types() {
        let value = json!({"ready": true, "count": 42});

        assert!(value_bool(&value, "ready"));
        assert!(!value_bool(&value, "missing"));
        assert_eq!(value_u64(&value, "count"), 42);
        assert_eq!(value_u64(&value, "ready"), 0);
    }

    #[test]
    fn failed_node_result_accepts_nested_or_plain_result() {
        assert!(is_failed_node_result(&json!({"result": {"failed": true}})));
        assert!(is_failed_node_result(&json!({"result": true})));
        assert!(!is_failed_node_result(
            &json!({"result": {"failed": false}})
        ));
    }

    #[test]
    fn summarize_network_counts_ready_and_status_names() {
        let nodes = vec![
            json!({"ready": true, "status": 4}),
            json!({"ready": false, "status": 3}),
            json!({"ready": false, "status": 99}),
            json!({"ready": false}),
        ];

        let summary = summarize_network(&nodes);

        assert_eq!(summary.ready_nodes, 1);
        assert_eq!(summary.status_counts["alive"], 1);
        assert_eq!(summary.status_counts["dead"], 1);
        assert_eq!(summary.status_counts["99"], 1);
        assert_eq!(summary.status_counts["unknown"], 1);
        assert_eq!(
            format_counts(&summary.status_counts),
            "99=1 alive=1 dead=1 unknown=1"
        );
    }

    #[test]
    fn controller_helpers_format_defaults() {
        const HOME_ID: u64 = 0x1abc;
        let controller = json!({"firmwareVersion": "1.2.3", "homeId": HOME_ID});

        assert_eq!(controller_firmware(&controller), "1.2.3");
        assert_eq!(controller_home_id(&controller), "00001abc");
        assert_eq!(controller_firmware(&json!({})), "unknown");
        assert_eq!(controller_home_id(&json!({})), "unknown");
    }

    #[test]
    fn node_label_prefers_name_then_device_config() {
        assert_eq!(
            node_label(&json!({
                "name": "Front Door",
                "deviceConfig": {"manufacturer": "Zooz", "label": "Sensor"}
            })),
            "Front Door (Zooz Sensor)"
        );
        assert_eq!(node_label(&json!({"name": "Front Door"})), "Front Door");
        assert_eq!(
            node_label(&json!({"deviceConfig": {"manufacturer": "Zooz", "label": "Sensor"}})),
            "Zooz Sensor"
        );
        assert_eq!(node_label(&json!({})), "-");
    }

    #[test]
    fn format_inclusion_event_uses_available_fields() {
        assert_eq!(
            format_inclusion_event(&json!({
                "event": "node added",
                "node": {"nodeId": 12},
                "status": "done"
            })),
            "node added: node=12 status=done"
        );
        assert_eq!(
            format_inclusion_event(&json!({"type": "step", "nodeId": 13})),
            "step: node=13"
        );
        assert_eq!(
            format_inclusion_event(&json!({"event": "done", "result": "ok"})),
            "done: status=ok"
        );
        assert_eq!(
            format_inclusion_event(&json!({"payload": true})),
            r#"message: {"payload":true}"#
        );
    }

    #[test]
    fn clap_definition_is_valid() {
        Cli::command().debug_assert();
    }
}
