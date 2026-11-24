# medre-adapter-lxmf

The LXMF adapter for MEDRE.

## Install

Use [uv](https://docs.astral.sh/uv/getting-started/installation/) to install
MEDRE and this adapter in the same persistent tool environment:

```bash
uv tool install medre
medre adapters install lxmf
```

On Windows, the package-changing command prints a uv command to run **after**
`medre.exe` exits, rather than replacing an executable still in use.
Adapter installation defaults to package-index requirements; existing saved
Git requests and catalogs are preserved. For fake smoke, guided route
creation, locked uv deployment projects, private Git sources, local checkouts,
and the same-environment requirement, see the
[MEDRE Getting Started guide](https://github.com/jeremiah-k/medre/blob/main/docs/ops/getting-started.md)
and the
[adapter-source guide](https://github.com/jeremiah-k/medre/blob/main/docs/ops/adapter-installation.md).
Neither MEDRE nor the adapter installs packages during runtime startup.

## Configure

Save a private `connections.yaml` with one real instance. The common case
needs the private storage path; Reticulum uses its default configuration
locations when `reticulum_config_dir` is omitted:

```yaml
adapters:
  lxmf:
    relay:
      adapter_kind: real
      connection_type: reticulum
      storage_path: /private/medre/lxmf/relay
routes: {}
```

Add another adapter instance, then build routes with guided configuration.
For identity handling, every option, environment overrides, and
troubleshooting, see the
[LXMF setup guide](https://github.com/jeremiah-k/medre/blob/main/docs/ops/transport-setup/lxmf.md).

## Verify

Confirm the installed adapter and the saved configuration resolve:

```bash
medre adapters check --config connections.yaml
```

The setup guide's
[Verify](https://github.com/jeremiah-k/medre/blob/main/docs/ops/transport-setup/lxmf.md#verify)
section shows the config check, health semantics, and first-contact path
discovery timing.

## Using an existing Reticulum setup

Reticulum is the shared network service; LXMF is the messaging protocol used by
applications such as Sideband and Reticulum MeshChat. MEDRE acts as its own LXMF
contact and exchanges ordinary text messages with those applications. A dedicated
Reticulum instance is optional. See the [Reticulum sharing guide](https://reticulum.network/manual/using.html).

Keep MEDRE's LXMF identity and storage profile separate from another chat app's
profile. Select the existing Reticulum configuration with `reticulum_config_dir`,
or leave it unset to use the SDK's normal configuration discovery. Leaving
`identity_path` unset lets MEDRE persist a dedicated identity in `storage_path`.
Applications can share the network service without sharing their private keys,
ratchets, or local message databases. Original relay senders are represented by
MEDRE's configured attribution text and metadata, rather than separate native
LXMF signing identities. Reply/thread relations use the text fallback described
below; app-specific attachments and reactions are separate capabilities.

An offline setup form collects these connection settings without starting a
runtime, importing the transport SDKs, or contacting the network:

```bash
medre config native --transport lxmf --adapter-id bot --output connection.yaml
```

The form writes a new private YAML file and collects configuration values only.
It references an existing Reticulum identity file by path and never provisions,
generates, or modifies identity material. Preparing that identity, configuring
Reticulum interfaces, and discovering peer destination hashes for route
contexts remain native, manual steps: use the
[Reticulum sharing guide](https://reticulum.network/manual/using.html) and the
announces of the peers to reach. Leaving the identity path blank keeps the
documented behavior above: the worker persists a dedicated identity in the
storage path on first start.

Stopped snapshots cover MEDRE's LXMF application identity, ratchets, router state,
and handoff journal. They do not capture or stop a shared Reticulum service or
other chat applications. Keep that network setup available, or restore it using
its own procedure. The [native-worker specification](docs/spec/native-worker.md#stopped-state-capture-scope)
describes the detailed capture boundary.

## Replies and threads

Replies and threads relay as inline fallback text. They do not emit native
reply/thread operations. Delivery evidence identifies `fallback_text`;
reactions, edits, and deletes remain unsupported and are suppressed.
Fake adapters expose the same planning capabilities as their health reports.

## Startup and shutdown ownership

Real Reticulum adapters run native SDK construction, queues and callbacks in an
owned worker process. A blocked constructor cannot block core startup or replace
MEDRE's host signal handlers. The parent retains canonical admission and exact
core delivery-attempt provenance; native terminal facts become ordinary
post-handoff observations. SDK-free fakes keep the local session.

Each worker exclusively leases its identity and queue storage. Omitting
identity_path generates a persistent identity under storage_path. Preserve that
private directory across restarts. The local handoff journal fences repeated
submission of the same core attempt; an uncertain native outcome requires
operator reconciliation and suppresses automatic retry. Handoff acceptance does
not establish recipient delivery. Worker loss does not invent terminal facts.

See the [native worker contract](docs/spec/native-worker.md) for cancellation,
queue bounds, environment inheritance, restart limits and qualification, and the
[runtime ownership contract](docs/spec/runtime-ownership.md) for SDK embedding.

## Development

See [contributing](CONTRIBUTING.md) for the shared Trunk quality gate and
uv-based editable development workflow. Maintainer material — packaging and
provenance, ownership and imports, offline samples and diagnostics, tests,
fake smoke sources, coordinated CI, and the adapter contract — lives in
[adapter maintenance](docs/dev/adapter-maintenance.md).
