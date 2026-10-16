# liyanlabs-relay

A self-hosted RustDesk-compatible rendezvous + relay server, written from
scratch and licensed AGPL-3.0-or-later. Designed to back the remote-desktop
feature for advanced users of [LiyanLabs](https://liyanlabs.com).

## Authenticated screen media

The deployed LiyanLabs screen path uses `liyanlabs-stream`. Configure
`LIYANLABS_MEDIA_ADMISSION_URL` with the environment's private API admission
base and `LIYANLABS_CONTROL_TOKEN` with its distinct server secret (at least
32 bytes). Missing configuration fails closed.

Publishers and viewers obtain device/generation/purpose-bound admission URLs
from the authenticated API. A peer ID never proves authority. Publisher
replacement fences the prior generation, closes its viewers, and clears its
cached frames. The existing Runner lease is checked once every 20 seconds per
publisher; failure closes the room. Media bytes never go through the business
API. Last-viewer idle time is 15 seconds.

`liyanlabs-capturer --publish-url <authorized-url>` is a device-local diagnostic.
Run `cargo test -p liyanlabs-relay-stream` for authenticated fan-out and
disconnect coverage. Never log or persist short-lived admission URLs.

The rendezvous, generic TCP relay and SQLite control crates below are separate
experiments. They are not part of the deployed camera/screen viewing path.

## Status

Pre-alpha. Goal of the first milestone is **wire-protocol parity with the
[`rustdesk-server-demo`](https://github.com/rustdesk/rustdesk-server-demo)** —
i.e. an unmodified official RustDesk client can connect to two peers through
this server via TCP relay (no NAT hole-punching yet).

## Layout

| Crate          | Role                                                                      |
| -------------- | ------------------------------------------------------------------------- |
| `proto`        | Generated Prost types from RustDesk's open `rendezvous.proto` + framing   |
| `common`       | Shared utilities (config, logging, address types)                         |
| `rendezvous`   | Replaces `hbbs` — peer registry + punch coordination                      |
| `relay`        | Replaces `hbbr` — bidirectional TCP relay                                 |

## License

This project is **AGPL-3.0-or-later**. RustDesk's wire protocol definitions
(`rendezvous.proto`) are vendored under `crates/proto/protos/` from upstream
`rustdesk/hbb_common`, also AGPL-3.0. Anyone running this server as a
network-accessible service is required by §13 to make the corresponding
source available to its users.

The LiyanLabs product itself is **not** a derivative of this code: it talks to
`liyanlabs-relay` only over a network API.
