use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const DEFAULT_URL: &str = "ws://zwave-api.localdomain/";

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
    }
}

impl ZwaveClient {
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

async fn cmd_rebuild_routes(client: &mut ZwaveClient, raw_json: bool) -> Result<()> {
    let result = client
        .command("controller.begin_rebuilding_routes", json!({}))
        .await?;
    print_result(raw_json, &result)
}

async fn start_listening(client: &mut ZwaveClient) -> Result<Value> {
    client.command("start_listening", json!({})).await
}

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

async fn read_json(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Result<Value> {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(30), socket.next())
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
