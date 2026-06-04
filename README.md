# zwave

Small Rust CLI for a Z-Wave JS Server websocket endpoint.

The default endpoint is `ws://zwave-api.localdomain/`. Override it with
`--url` or `ZWAVE_WS_URL`.

## Install

```bash
./deploy.sh
```

By default, `deploy.sh` installs with `cargo install` into
`/syncthing/Sync/Provisioning/bin`.

For a normal Cargo install:

```bash
cargo install --path .
```

## Usage

```bash
zwave status
zwave nodes
zwave nodes --dead
zwave dead
zwave ping 27
zwave is-failed 4
zwave remove-failed 4
zwave rebuild-routes
```

Use `--json` to print raw API responses where available:

```bash
zwave --json status
zwave --json dead
```

## API

This talks to the official Z-Wave JS Server websocket API, not the Z-Wave JS UI
Socket.IO admin API.
