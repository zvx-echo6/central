# Central — Producer Integration

How to add a new `SourceAdapter` subclass to Central. This is the contract for
adapter authors: what to implement, what NOT to implement, and how a new feed
plugs into the existing stream / subject / dedup machinery without restating
anything from `docs/CONSUMER-INTEGRATION.md`.

The companion document is [`CONSUMER-INTEGRATION.md`](./CONSUMER-INTEGRATION.md) — the contract
for downstream consumers. Producer-side semantics are in scope here; consumer-side
semantics (wire format, dedup recommendations, fall-off handling at the subscriber)
are intentionally not restated. Cross-references point into that doc.

## Table of contents

1. [What this doc is for](#1-what-this-doc-is-for)
2. [The design principle](#2-the-design-principle)
3. [Quick start: cloning an existing adapter](#3-quick-start-cloning-an-existing-adapter)
4. [The SourceAdapter base class](#4-the-sourceadapter-base-class)
5. [Settings and configuration](#5-settings-and-configuration)
6. [Subject namespace conventions](#6-subject-namespace-conventions)
7. [Dedup key construction](#7-dedup-key-construction)
8. [The StreamEntry registry](#8-the-streamentry-registry)
9. [Removal / fall-off patterns](#9-removal--fall-off-patterns)
10. [Anti-patterns — what NOT to do](#10-anti-patterns--what-not-to-do)
11. [Settings preview hook](#11-settings-preview-hook)
12. [Acceptance gate for a new adapter](#12-acceptance-gate-for-a-new-adapter)
13. [Enrichment contract](#13-enrichment-contract)

---

## 1. What this doc is for

**Audience:** an engineer (or a code-gen agent acting as one) writing a new
`SourceAdapter` subclass in `src/central/adapters/`.

**Covered:** the SourceAdapter contract, the StreamEntry registry, subject
namespace conventions, dedup-key construction, the settings preview hook, the
mechanical checklist a new-adapter PR must satisfy.

**NOT covered:** end-to-end walkthroughs of one specific adapter (read the source
for the closest existing adapter — `src/central/adapters/swpc_kindex.py` is the
smallest), upstream feed API documentation (lives upstream), or consumer-side
concerns (live in [`CONSUMER-INTEGRATION.md`](./CONSUMER-INTEGRATION.md)).

---

## 2. The design principle

> "Central takes it all and gives it all. It's up to the pipe to do with it
> what it will."
> — Matt, 2026-05-19

The correct reading of that sentence: **Central is the consumer's only data
plane.** A downstream consumer sees exactly what's on the wire and nothing
more — it cannot do a follow-up lookup, cannot re-query the upstream, cannot
reverse-geocode a coordinate on its own. So whatever Central does NOT put on
the wire is, for every consumer, simply missing. "Gives it all" therefore means
*give the consumer everything a reasonable consumer needs to act on the event*
— not "give the upstream payload only."

Adapter authors translate that into a small number of concrete rules:

- **Preserve every upstream field.** Anything the upstream returns lives in
  `Event.data` verbatim. Adapters do not silently drop fields, even ones that
  look redundant or low-value today (see [§10.2](#102-silent-field-dropping)).
- **Enrich, deliberately and centrally.** Location, timezone, elevation,
  landclass and similar context that consumers reliably need should be resolved
  once, by Central, and attached — not left for twelve consumers to each
  re-derive (most of them can't). Enrichment runs through the framework
  ([§13](#13-enrichment-contract)): an adapter declares `enrichment_locations`
  and the supervisor attaches results under `Event.data["_enriched"]`.
- **Namespace enrichment for provenance.** Central-derived fields live under
  `_enriched.<enricher_name>`; everything else in `data` is upstream verbatim.
  A consumer can always tell which is which.
- **Fail gracefully to null, never to an exception.** Enrichment that can't
  resolve a field returns `null` for it (a stable, documented field set), and a
  total enrichment failure returns an all-null bundle. A geocoder outage must
  never drop or corrupt the underlying event.
- **No opinionated translation of the upstream payload.** Enrichment *adds*
  namespaced fields; it does not rewrite upstream ones. Adapters still do not
  coerce units, rename upstream fields, or collapse upstream enumerations inside
  `data`. The only in-place adapter transforms remain mechanical: subject-token
  normalization (camelCase → snake_case, agency-prefix splitting, whitespace →
  underscore, lowercase) and dedup-key construction.

This reframes a Phase 2 rule. The earlier draft of this doc said "no
enrichment — that's consumer-side work," and a proposal to enrich NWIS rows was
rejected on those grounds. That reasoning is now inverted: consumers have no
practical way to do that work, so Central does it. The constraint that survives
is *where* and *how* — through the framework, namespaced, cached, failing to
null — not *whether*. See [§10.1](#101-enrichment-outside-the-framework) for the
remaining anti-pattern (enrichment done outside the framework) and
[§13](#13-enrichment-contract) for the full contract.

---

## 3. Quick start: cloning an existing adapter

The cheapest way to start a new adapter is to copy the smallest existing one
and edit. As of this writing the smallest is `swpc_kindex` (one fixed subject,
no region filter, no preview hook).

**File layout for a one-file adapter:**

```
src/central/adapters/<your_adapter>.py     # the SourceAdapter subclass
tests/test_<your_adapter>.py               # unit tests
```

When the upstream API needs shared utilities (e.g. WFIGS shares helpers across
`wfigs_incidents` and `wfigs_perimeters`), factor them into a sibling
`<family>_common.py` module — see `wfigs_common.py` and `swpc_common.py` for
the existing precedent.

**Minimum overrides:**

```python
from collections.abc import AsyncIterator
from pathlib import Path

import aiohttp
from pydantic import BaseModel

from central.adapter import SourceAdapter
from central.config_models import AdapterConfig
from central.config_store import ConfigStore
from central.models import Event, Geo


class ExampleSettings(BaseModel):
    # Adapter-tunable settings live here. Pydantic v2 model. Forbid extras
    # if you want strictness; otherwise extra-field silence is the default.
    pass


class ExampleAdapter(SourceAdapter):
    name = "example"
    display_name = "Example Source"
    description = "One-line operator-facing description."
    settings_schema = ExampleSettings
    requires_api_key = None
    api_key_field = None
    wizard_order = None
    default_cadence_s = 600

    def __init__(
        self,
        config: AdapterConfig,
        config_store: ConfigStore,
        cursor_db_path: Path,
    ) -> None:
        self._config_store = config_store
        self._cursor_db_path = cursor_db_path
        self._session: aiohttp.ClientSession | None = None

    async def startup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
        )

    async def shutdown(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def apply_config(self, new_config: AdapterConfig) -> None:
        # Re-derive any cached settings from new_config.settings.
        pass

    def subject_for(self, event: Event) -> str:
        return f"central.<domain>.example"

    async def poll(self) -> AsyncIterator[Event]:
        # Fetch, parse, yield Events. Dedup using a sqlite3 cursor_db
        # connection initialized in startup() — see swpc_kindex for the
        # established `published_ids` table shape.
        if False:
            yield  # type: ignore[unreachable]
        return
```

Auto-discovery picks the adapter up the moment the module imports cleanly. No
registry edit is required — `central.adapter_discovery.discover_adapters()`
walks `central.adapters.*` at import time and registers every concrete
`SourceAdapter` subclass keyed by its `name` class-attribute.

---

## 4. The SourceAdapter base class

`src/central/adapter.py` is the SoT. The contract is reproduced here verbatim
where the docstrings already capture it; this section adds the supervisor-side
context that the source file does not narrate.

### 4.1 Class attributes

Every subclass must define:

| Attribute | Type | Purpose |
|---|---|---|
| `name` | `str` | Short identifier, e.g. `"nws"`. Becomes the `adapter` key in `Event.adapter` and the discovery-registry key. Must be unique across `central.adapters.*`. |
| `display_name` | `str` | Human-readable name shown in the GUI. |
| `description` | `str` | Short description shown on the adapter detail page. |
| `settings_schema` | `type[BaseModel]` | Pydantic v2 model class for adapter settings. See [§5](#5-settings-and-configuration). |
| `default_cadence_s` | `int` | Default poll interval in seconds. Operators may override via `config.adapters`; `AdapterConfig.cadence_s` has a Pydantic `ge=10` floor. |

Optional class attributes (default to `None` / not-in-wizard):

| Attribute | Type | Purpose |
|---|---|---|
| `requires_api_key` | `str \| None` | Key alias if an API key is required, else `None`. |
| `api_key_field` | `str \| None` | Names the `settings_schema` field that holds the api_key alias reference. The GUI renders this as a select populated from `config.api_keys`; the wizard validates it against the staged `api_keys` state. |
| `wizard_order` | `int \| None` | Position in the setup wizard. `None` excludes the adapter from the wizard. |
| `data_class` | `Literal["event", "telemetry"]` | GUI classification. `"event"` (default) = discrete events, shown on `/events`. `"telemetry"` = continuous/high-volume feeds (e.g. NWIS) shown on `/telemetry` instead, so they don't drown discrete-event signal. GUI-only — does not affect publishing or the `events.json` contract. |

### 4.2 Required methods

Every subclass must implement these three abstract methods.

**`async def poll(self) -> AsyncIterator[Event]`**

> Poll the source for new events. Yields `Event` objects for each new/updated
> event found.

The supervisor drives the loop; `poll()` is a one-shot async generator the
supervisor calls once per cadence tick, drains, and discards. Adapters MUST NOT
loop internally — control of cadence and concurrency lives in the supervisor.

The supervisor wraps each yielded `Event` in a CloudEvents envelope and
publishes it. The adapter is responsible only for yielding `Event` objects;
all envelope construction, NATS publish, and metadata heartbeat emission live
outside the adapter.

**`async def apply_config(self, new_config: AdapterConfig) -> None`**

> Apply new configuration to the adapter. Called by supervisor when config
> changes via hot-reload. The adapter should extract relevant settings from
> `new_config.settings` and update its internal state.

`new_config.settings` is a `dict[str, Any]`; re-validate it against
`settings_schema` if you intend to read structured fields. Most adapters cache
a couple of fields (e.g. `region`, `parameter_codes`) on `self` during
`__init__` and refresh them here. Do NOT tear down `self._session` or
`self._db` here — that is `shutdown()`'s responsibility.

**`def subject_for(self, event: Event) -> str`**

> Compute the NATS subject for an event. Each adapter knows its own subject
> hierarchy. The supervisor calls this to determine where to publish each
> event.

The result must match the subject filter of exactly one `StreamEntry` in
`central.streams.STREAMS`. See [§6](#6-subject-namespace-conventions) for the
naming rules and [§8](#8-the-streamentry-registry) for the stream side.

### 4.3 Optional lifecycle hooks

**`async def startup(self) -> None`** — Optional. Called once before the first
`poll()`. Open the HTTP session, open the sqlite3 cursor DB, create any
required tables. The default is a no-op.

**`async def shutdown(self) -> None`** — Optional. Called once at graceful
shutdown. Close the HTTP session, close the sqlite3 cursor DB. The default is
a no-op.

**`def bump_last_seen(self, event_id: str) -> None`** — Optional. Called by the
supervisor on every dedup hit (an already-published event re-seen upstream) so
the periodic `sweep_old_ids` purge doesn't expire long-lived events. The base
class implements the standard `published_ids` bump; adapters using that table
inherit it and need not override. It is a safe no-op when the adapter has no
`_db` handle.

**`def is_published(self, event_id: str) -> bool`** — Optional. Returns whether
`event_id` is already in the `published_ids` dedup table. The base class
implements the standard query; adapters using that table inherit it. Safe
(returns `False`) when the adapter has no `_db` handle.

**`def mark_published(self, event_id: str) -> None`** — Optional. Records
`event_id` as published (refreshing `last_seen` on re-publish). Base-class
default operates on `published_ids`; safe no-op without `_db`.

**`def sweep_old_ids(self) -> int`** — Optional. Purges dedup rows older than
`dedup_sweep_days` (default 14; NWIS overrides to 30) and returns the count
deleted. Base-class default; safe no-op (returns 0) without `_db`.

**`async def preview_for_settings(self, settings: BaseModel) -> list[dict] | None`** —
Optional. The settings-page preview hook. The default returns `None` (no
preview). See [§11](#11-settings-preview-hook) for the contract.

### 4.4 Constructor signature

The supervisor instantiates adapters with this exact keyword-argument call
(`src/central/supervisor.py` `_create_adapter`):

```python
cls(
    config=config,                    # AdapterConfig — name, enabled, cadence_s, settings, …
    config_store=self._config_store,  # ConfigStore — staged-vs-live config access
    cursor_db_path=CURSOR_DB_PATH,    # Path — /var/lib/central/cursors.db
)
```

Adapters take exactly these three keyword arguments. Adding new constructor
parameters silently breaks supervisor instantiation; if you need additional
state, derive it from `config.settings` or read it lazily during `startup()`.

---

## 5. Settings and configuration

### 5.1 The settings schema

`settings_schema` is a Pydantic v2 `BaseModel` subclass. It is the SoT for
operator-tunable fields and the basis for GUI form rendering, wizard
validation, and migration seed defaults. Three rules:

- **Default values are SoT.** The settings model's defaults are the canonical
  starting state. Migrations seed `config.adapters.settings` from the same
  model; tests build fixtures by instantiating the model with no args. Do NOT
  duplicate default lists in migration JSON, fixtures, or test code — derive
  them from the model class.
- **Use `RegionConfig` for bounding boxes.** Defined in
  `central.config_models.RegionConfig`. Validates north/south/east/west,
  rejects zero-width bboxes, rejects antimeridian crossings. Reuse it; do not
  reinvent it.
- **Optional region.** Some adapters strongly recommend a region (NWIS, WFIGS)
  but accept `None` so an operator can test before configuring. Log a WARN at
  `startup()` if a region is recommended but unset; do not refuse to start.

### 5.2 What gets stored where

| State | Location | Adapter access |
|---|---|---|
| Adapter config (cadence, settings, enabled flag, paused state) | DB table `config.adapters`, fronted by `ConfigStore` | Passed in `__init__` as `config: AdapterConfig`; refreshed via `apply_config()` |
| API keys | DB table `config.api_keys`, fronted by `ConfigStore` | Read via `self._config_store.get_api_key(alias)`; never persisted on the adapter |
| Dedup cursor / observed sets | sqlite3 at `cursor_db_path` (`/var/lib/central/cursors.db`) | Each adapter creates its own table(s); the file is shared across adapters by convention but per-adapter rows are namespaced by the `adapter` column |
| Stream retention / max_bytes | DB table `config.streams`; managed by supervisor | Adapters do not read or write stream config |

`AdapterConfig.settings` is a `dict[str, Any]` — re-validate against
`settings_schema` before consuming structured fields. Most adapters do this
once in `__init__` and again in `apply_config()`.

### 5.3 preview_for_settings boundary

`preview_for_settings()` is the one place that does NOT have access to
`self._config_store` or `cursor_db_path` semantics. See
[§11](#11-settings-preview-hook). The framework may instantiate adapters
with a stub config store solely to call this method, and the GUI process never
calls `startup()`. Do not assume the adapter's HTTP session, cursor DB, or any
other startup-time state exists when preview runs.

---

## 6. Subject namespace conventions

### 6.1 The shape

Every subject starts with `central.` and decomposes as:

```
central.<domain>.<subtype>[.<dimensions>...]
```

- `<domain>` is one of `wx`, `fire`, `quake`, `space`, `disaster`, `hydro`,
  `traffic`, `meta` (the current set — see [§8](#8-the-streamentry-registry) for adding
  one). Operators MUST be able to subscribe to all of one domain with
  `central.<domain>.>`.
- `<subtype>` is adapter-driven and identifies the event category within the
  domain (e.g. `hotspot`, `alert`, `incident`, `kindex`, `proton_flux`).
- `<dimensions>` are zero or more refinement tokens — geographic, temporal,
  upstream identifier — that the consumer would plausibly want to filter on.

Token rules:

- **All lowercase.** No uppercase, no mixed-case.
- **`snake_case` tokens.** Multi-word tokens use underscores
  (`proton_flux`, `sea_lake_ice`), not hyphens.
- **No empty tokens.** Fall back to a literal `unknown` token when a dimension
  is genuinely absent (NWS does this for alerts lacking a primary region).
- **No `.removed` injected mid-token.** Removal subjects always read
  `<subtype>.removed.<region>` — see [§9](#9-removal--fall-off-patterns).

### 6.2 Where the canonical subjects live

`docs/CONSUMER-INTEGRATION.md` §4 is the SoT for the concrete subject patterns
that exist today. Adapter authors should mirror new subjects there as part of
the same PR; the consumer test suite asserts every discovered adapter has a
per-adapter subsection.

### 6.3 Subject-token decomposition belongs in the adapter

When an upstream identifier is composite — agency-prefix + bare site number,
state + county, satellite + confidence — the adapter is responsible for
splitting it into tokens. Two conventions:

- **One helper function per decomposition, one place.** NWIS' helper sits at
  module scope in `nwis.py`:

  ```python
  def _subject_tokens_for_id(monitoring_location_id: str) -> tuple[str, str]:
      """Decompose an agency-prefixed monitoring_location_id into (agency, bare_site_no)."""
      ...
  ```

  Both `subject_for()` and `Event.category` construction call through it. WFIGS
  does the same with `subject_suffix()` in `wfigs_common.py`. Never inline the
  decomposition in two places — they will drift.

- **Round-trip via `Event.category`.** Adapters set `Event.category` to a
  Central-relative form (`hydro.<param>.<agency>.<bare_site_no>`,
  `fire.incident.<state>.<county>`); `subject_for()` then prepends `central.`
  and may rearrange tokens. The category lives in the wire payload (see
  [`CONSUMER-INTEGRATION.md` §5b](./CONSUMER-INTEGRATION.md#5b-inner-event-payload));
  the subject is computed from it at publish time.

### 6.4 Adding a new top-level domain

Top-level domains are stream-mapped 1:1 (see [§8](#8-the-streamentry-registry)).
Adding `central.<new_domain>.>` requires:

1. One new `StreamEntry` in `src/central/streams.py`.
2. One new migration row that seeds `config.streams` with retention and
   `max_bytes`.
3. Update `docs/CONSUMER-INTEGRATION.md` §3 and §4 to advertise the new
   stream / subject pattern.

`supervisor.STREAM_SUBJECTS`, `archive.STREAMS`, and `gui.DASHBOARD_STREAMS`
all derive from `STREAMS` at import time; no code edits in those modules are
required.

---

## 7. Dedup key construction

### 7.1 What "dedup key" means here

Adapters maintain a per-adapter sqlite3 table at `cursor_db_path` (typically
`published_ids` keyed by `(adapter, event_id)`) to suppress re-yielding the
same event across successive `poll()` runs. The string the adapter writes to
`published_ids.event_id` is the **adapter-side dedup key**.

Separately, every yielded `Event.id` flows into the CloudEvents envelope's
`id` field (also set as the `Nats-Msg-Id` header on publish). Most adapters
use the same string for both — adapter-side dedup key and `Event.id` are
identical — which keeps consumer-side dedup ([§9 of CONSUMER-INTEGRATION.md](./CONSUMER-INTEGRATION.md#9-dedup-implementation-guide))
trivial.

A few adapters intentionally diverge: `eonet` stores a richer composite
adapter-side (id + timeline date) while keeping `Event.id` stable, because
consumers usually want timeline updates to overwrite the same id. The
`Event.id` is always the consumer contract; adapter-side composites are an
implementation detail.

### 7.2 Two shapes

**Single-token.** Upstream provides a stable, globally-unique identifier:

- `usgs_quake` — USGS feature id (`ci10240102`)
- `gdacs` — RSS guid (`WF1028708`)
- `wfigs_incidents` / `wfigs_perimeters` — IRWIN UUID
- `inciweb` — numeric incident id
- `swpc_kindex` — bin timestamp (single fixed subject, time is the natural key)
- `nws` — alert URL
- `eonet` — `EONET_NNNNN` (consumer-facing); composite tracked separately
  adapter-side as described in §7.1

Use the upstream id directly. No prefix, no separators.

**Composite.** Upstream id is not by itself sufficient — needs a parameter, a
timestamp, or a tombstone marker to dedup correctly:

| Adapter | Shape |
|---|---|
| `firms` | `<satellite>:<acq_date>:<acq_time>:<lat_3dp>:<lon_3dp>` |
| `nwis` | `nwis:<monitoring_location_id>:<parameter_code>:<time_iso>` |
| `wfigs_incidents` / `wfigs_perimeters` removals | `<IrwinID>:removed:<iso_now>` |

### 7.3 Separator convention

Use `:` as the dedup-key separator for new adapters.

There is a pre-existing wrinkle: two SWPC adapters use `|` instead.

| Adapter | Separator | Shape |
|---|---|---|
| `swpc_alerts` | `\|` | `<product_id>\|<issue_timestamp>` |
| `swpc_protons` | `\|` | `<time_tag>\|<energy>` |

This is the historical reality at the time of writing — not a recommendation.
Do not propagate `|` to new adapters; do not refactor the SWPC pair to `:`
without an independent rename PR (the strings are persisted in
`/var/lib/central/cursors.db` rows).

### 7.4 Implementation

Every existing adapter follows the same pattern:

```python
async def startup(self) -> None:
    ...
    self._db = sqlite3.connect(self._cursor_db_path)
    self._db.execute("""
        CREATE TABLE IF NOT EXISTS published_ids (
            adapter TEXT NOT NULL,
            event_id TEXT NOT NULL,
            first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (adapter, event_id)
        )
    """)
    self._db.execute("""
        CREATE INDEX IF NOT EXISTS published_ids_last_seen
        ON published_ids (last_seen)
    """)
    self._db.commit()
```

Then `is_published(event_id) -> bool`, `mark_published(event_id) -> None`, and
a periodic `sweep_old_ids()` (typical retention: 7 to 30 days, tuned per
adapter). Copy from `swpc_kindex.py` — it is the canonical reference shape.

### 7.5 The CloudEvents `id` field

`Event.id` becomes the CloudEvents envelope `id` and the `Nats-Msg-Id` header.
JetStream applies its own short dedup window keyed on `Nats-Msg-Id`, but
consumers do their own dedup ([CONSUMER-INTEGRATION.md §9](./CONSUMER-INTEGRATION.md#9-dedup-implementation-guide))
and should not rely on JetStream's window. Stability of `Event.id` across
re-publishes is the producer's contract; the consumer side trusts it.

---

## 8. The StreamEntry registry

`src/central/streams.py` is the SoT for stream definitions. The whole file is
short enough to quote:

```python
@dataclass(frozen=True)
class StreamEntry:
    name: str
    subject_filter: str
    event_bearing: bool = True
    dashboard: bool = True


STREAMS: list[StreamEntry] = [
    StreamEntry("CENTRAL_WX",       "central.wx.>"),
    StreamEntry("CENTRAL_FIRE",     "central.fire.>"),
    StreamEntry("CENTRAL_QUAKE",    "central.quake.>"),
    StreamEntry("CENTRAL_SPACE",    "central.space.>"),
    StreamEntry("CENTRAL_DISASTER", "central.disaster.>"),
    StreamEntry("CENTRAL_HYDRO",    "central.hydro.>"),
    StreamEntry("CENTRAL_TRAFFIC",  "central.traffic.>"),
    StreamEntry("CENTRAL_META",     "central.meta.>", event_bearing=False),
]
```

### 8.1 What derives from STREAMS

Three import-time consumers read `STREAMS` and build their own structures:

| Derived constant | Module | Shape |
|---|---|---|
| `STREAM_SUBJECTS` | `central.supervisor` | `dict[str, list[str]]` — used to create JetStream streams |
| `STREAMS` | `central.archive` | `list[tuple[str, str]]` for `event_bearing=True` entries only |
| `DASHBOARD_STREAMS` | `central.gui.routes` | `list[str]` for `dashboard=True` entries |

`tests/test_stream_registry.py` asserts each derivation matches the registry —
adding a stream automatically extends the test surface; no new test code is
required.

### 8.2 Adding a stream

When a new adapter needs a new top-level domain:

1. **One line in `src/central/streams.py`.** Append a `StreamEntry(...)` to
   the `STREAMS` list.
2. **One migration row.** Seed `config.streams` with retention and
   `max_bytes`. Existing migrations in `migrations/` are the template.

`event_bearing=False` is reserved for `CENTRAL_META` today; adding a second
non-event-bearing stream silently excludes it from the archive. The registry
test explicitly guards against this and requires a deliberate test update if
the invariant changes.

### 8.3 The consistency gate

`tests/test_stream_registry.py` is the property-test gate. It asserts:

- Stream names and subject filters are unique.
- Every subject filter matches `^central\.[a-z][a-z_]*\.>$`.
- `CENTRAL_META` is the only non-event-bearing stream.
- The supervisor / archive / GUI derivations all match the registry.

Mirror that discipline in any new test you write for adapter-side stream
behavior: derive expected values from `STREAMS` rather than hand-typing them.

---

## 9. Removal / fall-off patterns

When an upstream record disappears, an adapter has two options:

### 9.1 Explicit removal subject

Emit an `Event` whose subject is `<subtype>.removed.<region>` and whose
`category` ends in `.removed`. This is the right choice when:

- Upstream signals retirement explicitly (`gdacs`'s `iscurrent: false`).
- The upstream is a "current state" feed and absence is meaningful
  (`wfigs_incidents`, `wfigs_perimeters`, `eonet`).

The subtype-before-removed token order is a hard convention — consumers
filtering on `central.<domain>.<subtype>.>` receive both live and removal
events with a single subscription. See
[`CONSUMER-INTEGRATION.md` §7a](./CONSUMER-INTEGRATION.md#7a-explicit-removal-subjects-consumer-must-subscribe-to-handle-cleanly)
for the consumer side.

### 9.2 Absence-as-signal

No removal subject; consumers infer disappearance from a time gap or an
upstream-specific marker (`expires` field, supersession text). This is the
right choice when:

- The upstream is a stream of point-in-time observations (`firms` hotspot
  pixels, `swpc_kindex` 3-hour bins, `swpc_protons` cadence-driven samples).
- Upstream signals expiry inline (`nws` `expires` field, `nws`
  `messageType: Cancel`).
- The upstream is durable / append-only (`usgs_quake` catalog, `inciweb` RSS).

See [`CONSUMER-INTEGRATION.md` §7b](./CONSUMER-INTEGRATION.md#7b-absence-as-signal-no-removal-subject--consumer-infers-from-time-gap)
for the consumer-side enumeration.

### 9.3 Composite removal dedup key

When an adapter emits explicit removals, the same upstream id can fall off
and re-appear multiple times over an adapter's lifetime. The dedup key for a
removal event therefore needs a removal-detection timestamp:

```python
Event(
    id=f"{irwin_id}:removed:{now.isoformat()}",
    category="fire.incident.removed",
    # ...
)
```

The `:removed:<iso_now>` suffix is the cross-adapter convention. Keep it.

### 9.4 Removal payload schema

Removal events ship an empty `geo` block and a sparse `data` payload. The
exact shape (id field name, reason values, last_observed_at, adapter-specific
context fields) is documented in
[`CONSUMER-INTEGRATION.md` §7c](./CONSUMER-INTEGRATION.md#7c-removal-event-payload-schema-cross-cutting);
adapter authors should mirror it. Do not restate it here.

---

## 10. Anti-patterns — what NOT to do

These are the patterns prior reviews have explicitly rejected. Reject them
again on sight in a new-adapter PR.

### 10.1 Enrichment outside the framework

Enrichment itself is **expected**, not forbidden — see
[§2](#2-the-design-principle) and [§13](#13-enrichment-contract). Any adapter
with location data should opt in by declaring `enrichment_locations` on the
adapter class; the supervisor then runs the registered enrichers and attaches
results under `Event.data["_enriched"]`.

The anti-pattern is doing enrichment the *wrong* way — outside the framework:

- An `if missing: await fetch_metadata()` branch buried in an adapter's
  `poll()`. This bypasses the cache (so every poll re-hits the geocoder), skips
  the `_enriched` namespacing (so consumers can't tell upstream from
  Central-derived), and gives up the never-raise/all-null safety net (so a
  geocoder hiccup can take down the poll).
- Writing enriched fields directly into the top level of `Event.data` instead
  of under `_enriched`. That destroys provenance — a consumer can no longer
  tell which fields came from the upstream feed and which Central added.
- Standing up a parallel enrichment path (a second HTTP client, a private cache)
  inside one adapter instead of registering a backend with the framework.

The rule of thumb: declare `enrichment_locations`, let the supervisor do the
work. If the framework can't express what you need, extend the framework
([§13](#13-enrichment-contract)) — don't route around it inside an adapter.

### 10.2 Silent field dropping

`Event.data` carries the upstream payload verbatim. Do not omit a field
because it "looks redundant" or "isn't useful right now." Operators
debugging a downstream consumer often need every field the upstream sent;
dropping fields makes that impossible after the fact.

The two acceptable transforms — token normalization (case, separators) and
dedup-key construction — are not field-drops. They produce new fields
derived from the upstream payload; the upstream payload itself is preserved.

### 10.3 Magic-number asserts in tests

Adapter tests that pin event counts to literal integers (`assert
len(events) == 47`) break the next time the upstream's fixture is
refreshed and contribute zero diagnostic value. Either assert structural
invariants (every yielded event has a non-empty `id`; subjects match the
adapter's `subject_for()` output; etc.) or assert against a count computed
from the input fixture (`assert len(events) == len(fixture.features)`).

### 10.4 Hardcoded stream / adapter lists in tests

Tests that enumerate `["CENTRAL_WX", "CENTRAL_FIRE", ...]` or `["firms",
"inciweb", ...]` as literals drift the moment a stream or adapter is added
or removed. Derive from the SoT:

```python
from central.streams import STREAMS
from central.adapter_discovery import discover_adapters

stream_names = {s.name for s in STREAMS}
adapter_names = set(discover_adapters().keys())
```

`tests/test_stream_registry.py` and `tests/test_consumer_doc.py` are the
in-tree templates. The sibling test for this doc
(`tests/test_producer_doc.py`) follows the same rule.

### 10.5 Persistent state outside `cursor_db_path`

Adapters MUST NOT write to disk outside the cursor DB at
`cursor_db_path`. No JSON files in `/var/lib/central/<adapter>/`, no
state stashed in `/tmp`. Multi-process invariants (the supervisor may
restart an adapter; the GUI may instantiate it independently to call
`preview_for_settings()`) only hold for the cursor DB and `config.adapters`.

### 10.6 Blocking calls in async paths

`poll()`, `apply_config()`, `startup()`, `shutdown()`, and
`preview_for_settings()` are async. Use `aiohttp`, not `requests`; use
`asyncio.to_thread()` to push CPU-bound or genuinely blocking work
(`shapely` geometry operations, large JSON parsing) off the event loop.
The supervisor runs every adapter on a single event loop — one blocked
adapter stalls every other adapter.

### 10.7 Reaching into framework internals from `preview_for_settings`

`preview_for_settings()` is a pure function of `settings`. Do NOT access
`self._config_store`, `self._cursor_db_path`, or any state established
in `startup()`. The framework may instantiate the adapter with a stub
config store solely to call this method. See [§11](#11-settings-preview-hook).

### 10.8 Hardcoded subject strings in tests

A test that asserts `subject == "central.fire.incident.mt.flathead"` for a
specific fixture is fine; a test that hardcodes the subject pattern as a
string and re-derives it instead of calling the adapter's
`subject_for(event)` is the wrong shape. Round-trip through the adapter so
the test breaks the day someone shifts a token.

---

## 11. Settings preview hook

`preview_for_settings()` is the optional GUI-side hook that surfaces a
settings-driven preview on the adapter edit page. NWIS uses it to show the
monitoring-locations inside the configured bbox before the operator hits
save; most adapters opt out by inheriting the base class's `return None`.

### 11.1 The contract (verbatim from `central.adapter.SourceAdapter`)

> Optional. Override to surface a settings-driven preview on the edit page.
>
> Return `list[dict]` (framework renders as a generic table; columns come from
> the first dict's keys, in insertion order). Return `None` to skip preview.
> Raise to surface an error banner — framework catches at the route boundary.
>
> Contract:
> - Preview is a pure function of `settings`. Do NOT access
>   `self._config_store` or `cursor_db` state — the framework may instantiate
>   adapters with a stub config_store solely to call this method.
> - Network preview implementations must open their own short-lived
>   `aiohttp` session (the adapter's polling session may not exist; the GUI
>   process never calls `startup()`).
> - Return `None` when preview is not meaningful (e.g., required settings
>   like region are unset). Return `[]` explicitly if the query ran and
>   matched zero rows — the framework renders that distinctly from `None`.

### 11.2 The three return states

| Return | Framework renders |
|---|---|
| `None` | Nothing — no preview block on the edit page |
| `[]` | `"Preview (0 rows)"` legend, no table |
| `list[dict]` with rows | Generic table; columns from the first dict's keys in insertion order; legend `"Preview (N rows)"` |

`tests/test_preview_hook.py` is the test surface for the partial; mirror its
discipline for any adapter-specific preview test.

### 11.3 Generic rendering — no per-adapter Jinja

The framework is duck-typed on `list[dict]`. Column headers are the first
dict's keys. There is no per-adapter branch in the template, and no
adapter-name conditionals are accepted on review. If an adapter's preview
needs a column the framework does not surface, the adapter rewrites the dict
shape, not the template.

### 11.4 Implementation pattern

NWIS is the canonical example. Skeleton:

```python
async def preview_for_settings(self, settings: NWISSettings) -> list[dict] | None:
    if settings.region is None:
        return None
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=15),
    ) as session:
        async with session.get(url, headers=...) as resp:
            resp.raise_for_status()
            page = await resp.json()
    return [
        {"site_id": ..., "name": ..., "site_type": ..., "state": ...}
        for feat in page.get("features") or []
    ]
```

Key points:

- Fresh `aiohttp.ClientSession` per call — `self._session` may not exist.
- Short timeout (NWIS uses 15s) — the edit page must render quickly.
- Small row cap (NWIS uses 50) — the preview is for orientation, not bulk
  inspection.
- Raise on HTTP / parse failure — the framework catches at the route boundary
  and renders an error banner.

---

## 12. Acceptance gate for a new adapter

A new-adapter PR is accepted when every item below holds. The list is
mechanical; reviewers and the PR author should both run through it before
requesting / granting merge.

- [ ] **Upstream curl evidence captured.** A real HTTP transcript (curl
      command + redacted response body) is pinned somewhere in the PR
      description or a fixture file. Documents what the adapter was written
      against.
- [ ] **`SourceAdapter` subclass conforms to [§4](#4-the-sourceadapter-base-class).**
      Class attributes set, three abstract methods implemented, constructor
      signature matches the supervisor call exactly.
- [ ] **`tests/test_<adapter>.py` exists.** Unit tests for the adapter; no
      magic-number asserts ([§10.3](#103-magic-number-asserts-in-tests)); no
      hardcoded stream / adapter lists
      ([§10.4](#104-hardcoded-stream--adapter-lists-in-tests)).
- [ ] **Dry-run on CT104 passes.** The supervisor instantiates the adapter
      without errors; one full `poll()` cycle yields events without raising.
- [ ] **Dedup keys stable across multiple ticks.** Two successive `poll()`
      runs against a frozen upstream fixture yield the same event-count delta
      as expected (zero new events on the second run if upstream is
      unchanged).
- [ ] **No hardcoded values that should be derived.** Subject prefixes,
      stream names, default parameter lists, etc., all derive from the
      registry / settings model / discovery surface.
- [ ] **`CONSUMER-INTEGRATION.md` updated.** New per-adapter subsection in
      §6, new subject row in §4, new stream row in §3 if a new domain was
      introduced. `tests/test_consumer_doc.py` passes.
- [ ] **`PRODUCER-INTEGRATION.md` mentions the adapter only if it
      introduces a new convention.** This doc is contract-shaped, not
      catalog-shaped — most new adapters need no edit here.
- [ ] **Full pytest suite green.**

---

## 13. Enrichment contract

Enrichment is how Central adds consumer-needed context (location names,
timezone, elevation, landclass, …) that the upstream feed doesn't carry and a
downstream consumer can't look up itself. It runs in the supervisor, after
dedup and before the CloudEvents wrap, for any adapter that opts in. Results are
namespaced under `Event.data["_enriched"]` so provenance stays explicit:
everything under `_enriched` is Central-derived; everything else in `data` is
upstream verbatim.

### 13.1 Opting an adapter in

Declare `enrichment_locations` on the adapter class — a list of
`(lat_field, lon_field)` tuples naming top-level keys in `Event.data`:

```python
class FIRMSAdapter(SourceAdapter):
    enrichment_locations = [("latitude", "longitude")]
```

Empty (the default on `SourceAdapter`) means "no enrichment, publish as-is."
The supervisor uses the first tuple that resolves to a non-null coordinate pair,
runs each registered enricher over `{"lat": …, "lon": …}`, and attaches the
results. No adapter code calls enrichers directly.

### 13.2 The `Enricher` Protocol

An enricher is any object satisfying this Protocol (`central.enrichment.base`):

- `name: str` — short identifier, used as the key under
  `Event.data["_enriched"]`.
- `async def enrich(self, location: dict[str, float]) -> dict[str, Any]` —
  given `{"lat": float, "lon": float}`, return a flat dict of enrichment
  fields. Fields it can't resolve are present with value `None` (not omitted).
  **Must never raise** — implementations handle their own failures and return
  an all-null bundle on total failure.

### 13.3 `GeocoderEnricher` and the `GeocoderBackend` Protocol

`GeocoderEnricher` (`central.enrichment.geocoder`, `name = "geocoder"`) is the
only enricher today. It owns the cache and the canonical field normalization;
the actual reverse-geocode is delegated to a pluggable backend satisfying the
`GeocoderBackend` Protocol:

- `async def reverse(self, lat: float, lon: float) -> dict[str, Any]` — return
  the canonical geocoder fields (see [§13.5](#135-per-field-coverage)); fields
  the backend can't resolve return `None`. Must never raise.

Backends shipped: `NaviBackend` (composed Navi `/api/reverse/<lat>/<lon>`
endpoint — name/address + timezone + landclass + elevation in one call),
`PhotonBackend` (raw Photon, name/address only), `NominatimBackend` (OSM
Nominatim, name/address only, with a configurable rate limit + `User-Agent`),
and `NoOpBackend` (all-null — the default until an operator configures a real
backend).

### 13.4 Cache + failure semantics

`GeocoderEnricher` is backed by a sqlite cache (`central.enrichment.cache`,
`/var/lib/central/enrichment_cache.db`):

- Key: `(enricher_name, lat_rounded, lon_rounded)`, coordinates rounded to 4
  decimal places (~11 m). TTL is per-enricher, default 24h.
- **Cache hit** → return cached bundle, no backend call.
- **Cache miss** → call backend, cache the normalized result (**even an
  all-null bundle** — so known-empty coordinates aren't re-hammered), return it.
- **Backend raises** (a violation of the never-raise contract, or an
  infrastructure error the backend chose to surface) → return an all-null
  bundle and **do not cache** it, so the next event for that coordinate retries.

Enrichment config (`EnrichmentConfig`: `enricher_class`, `backend_class`,
`backend_settings`, `cache_ttl_s`) is read once at supervisor startup. Changing
the enricher set is a restart, not a hot-reload.

### 13.5 Per-field coverage

The canonical geocoder bundle is exactly nine fields. They mirror
`central.enrichment.geocoder.GEOCODER_FIELDS` (the single source of truth — this
table must match it):

| Field | US events | Non-US events today | Non-US after Photon planet expansion |
|---|---|---|---|
| `name` | populated (wilderness sparsity gaps) | null | populated |
| `city` | populated (wilderness sparsity gaps) | null | populated |
| `county` | populated (wilderness sparsity gaps) | null | populated |
| `state` | populated (wilderness sparsity gaps) | null | populated |
| `country` | populated (wilderness sparsity gaps) | null | populated |
| `postal_code` | populated (wilderness sparsity gaps) | null | populated |
| `timezone` | populated | populated (`tz_world` is planet-scale) | populated |
| `landclass` | populated where PAD-US covers | null (PAD-US is US-only) | null |
| `elevation_m` | populated | populated (planet-DEM) | populated |

**Net for v0.5.0:** US events get a rich bundle; non-US events get `timezone` +
`elevation_m` and the rest null. Photon planet expansion is queued on the Navi
side with no firm ETA; when it lands, `NaviBackend` picks it up automatically
with zero Central code changes.

### 13.6 Known wrinkle — landclass antimeridian false-positive

`landclass` is derived from a PostGIS `ST_Intersects` against PAD-US polygons.
Points near 51–53°N **outside** the US can spuriously match the Aleutian "Rat
Islands" PAD-US polygon (false matching across the antimeridian), yielding a
non-null `landclass` that doesn't actually apply. This is a Navi-side bug being
worked separately; until it's fixed, treat a non-null `landclass` on a clearly
non-US point as suspect. Documented for consumers in
`CONSUMER-INTEGRATION.md`.

---
