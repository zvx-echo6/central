# Phase 1a-3 Verification Evidence

## T0 Baseline (TOML config mode, post-merge deploy)

**Timestamp:** 2026-05-16T03:10:51Z

### Upstream Alert IDs
```json
[
  "urn:oid:2.49.0.1.840.0.e22a439ed29ed11e4b3686d9fac419ce7ad40059.001.1",
  "urn:oid:2.49.0.1.840.0.b7acbf4f0381fb83c1b3f732a4ac9ca16a6204d1.002.1",
  "urn:oid:2.49.0.1.840.0.e420a03d4bb13559e9bd61c714d8753fa6a4f66d.001.1",
  "urn:oid:2.49.0.1.840.0.82fc471559645fcc3fefe49b4855bde43a7dde2b.001.1",
  "urn:oid:2.49.0.1.840.0.add970d087c8d383436ee5958fc56100408aaf2e.001.1",
  "urn:oid:2.49.0.1.840.0.f620e3599001fc9937324d55df89b55e475c5568.001.1",
  "urn:oid:2.49.0.1.840.0.f620e3599001fc9937324d55df89b55e475c5568.002.1",
  "urn:oid:2.49.0.1.840.0.dbde432f293a71618bf9908e5adcf9e5dd27e27c.006.1",
  "urn:oid:2.49.0.1.840.0.dbde432f293a71618bf9908e5adcf9e5dd27e27c.001.1",
  "urn:oid:2.49.0.1.840.0.dbde432f293a71618bf9908e5adcf9e5dd27e27c.003.1",
  "urn:oid:2.49.0.1.840.0.dbde432f293a71618bf9908e5adcf9e5dd27e27c.001.2",
  "urn:oid:2.49.0.1.840.0.dbde432f293a71618bf9908e5adcf9e5dd27e27c.005.1",
  "urn:oid:2.49.0.1.840.0.b5173bc4f407f3889ea8e9284af261796d04972b.002.1",
  "urn:oid:2.49.0.1.840.0.18277c28967847fb1b9e61f5afc236e42659e27b.001.1",
  "urn:oid:2.49.0.1.840.0.b5173bc4f407f3889ea8e9284af261796d04972b.001.1",
  "urn:oid:2.49.0.1.840.0.86299b43bf001e6c38df077a9b2d8d8e1e7b9116.002.2"
]
```

### Database State
```
 count |          max
-------+------------------------
    30 | 2026-05-16 02:45:00+00
```

### Fresh Envelope Sample (post-restart)
```json
{
  "id": "https://api.weather.gov/alerts/urn:oid:2.49.0.1.840.0.35f852d42f3149d3e1722c14e6ffc2e977e48d1b.001.1",
  "source": "central/adapters/nws",
  "type": "central.wx.alert.lake_wind_advisory.v1",
  "time": "2026-05-16T02:45:00+00:00",
  "datacontenttype": "application/json",
  "centralschemaversion": "1.0.0",
  "centralcategory": "wx.alert.lake_wind_advisory",
  "centralseverity": 2,
  "specversion": "1.0",
  "data": { ... }
}
```

**CloudEvents verification:**
- `specversion: "1.0"` ✅
- `type` starts with `central.` (NOT `hub.`) ✅
- Extension attributes use `central*` prefix ✅
  - `centralschemaversion` (NOT `hubschemaversion`)
  - `centralcategory` (NOT `hubcategory`)
  - `centralseverity` (NOT `hubseverity`)

---

## Phase B Step 2: Config Source Cutover (TOML → DB)

**Timestamp:** 2026-05-16T03:13:33Z

### Environment Change
```
# /etc/central/central.env - added:
CENTRAL_CONFIG_SOURCE=db
```

### Supervisor Journal Evidence
```json
{"ts": "2026-05-16T03:13:33.430635+00:00", "level": "INFO", "logger": "central.supervisor", "msg": "Config source: db", "config_source": "db"}
{"ts": "2026-05-16T03:13:33.460162+00:00", "level": "INFO", "logger": "central.config_store", "msg": "Config listener connected to database"}
```

### Archive Journal Evidence
```json
{"ts": "2026-05-16T03:14:03.413008+00:00", "level": "INFO", "logger": "central.archive", "msg": "Archive starting", "nats_url": "nats://localhost:4222", "config_source": "db"}
```

**Result:** Both services running with DB-backed config ✅

---

## Phase B Step 3: Hot-Reload Cadence Test

**Test:** Change cadence from 60s → 90s while adapter is running.
**Goal:** Verify next poll is at Tlast + new_cadence (not old cadence, not immediate).

### Timeline
```
Tlast (last poll):     03:16:34.317219Z
Config change:         03:16:36Z
Expected next poll:    03:18:04.317Z (Tlast + 90s)
Actual next poll:      03:18:04.502Z ✅
```

### Journal Evidence
```json
{"ts": "2026-05-16T03:16:34.317219+00:00", "level": "INFO", "logger": "central.adapters.nws", "msg": "NWS yielded events", "count": 16}
{"ts": "2026-05-16T03:16:37.488781+00:00", "level": "INFO", "logger": "central.supervisor", "msg": "Config change received", "table": "adapters", "key": "nws"}
{"ts": "2026-05-16T03:16:37.511029+00:00", "level": "INFO", "logger": "central.supervisor", "msg": "Rescheduled adapter", "adapter": "nws", "old_cadence_s": 60, "new_cadence_s": 90, "next_poll": "2026-05-16T03:18:04.317651+00:00"}
{"ts": "2026-05-16T03:18:04.502991+00:00", "level": "INFO", "logger": "central.adapters.nws", "msg": "NWS poll completed", "status": 200, "feature_count": 355}
```

**Result:** Rate-limit guarantee upheld. Poll occurred at Tlast + 90s (NOT Tlast + 60s). ✅

---

## Phase B Step 4: Hot-Reload Enable/Disable Test

**Test:** Disable adapter, wait, re-enable.
**Goal:** Verify next poll is at Tlast + cadence (not immediate on re-enable).

### Timeline
```
Tlast (last poll):     03:19:34.758524Z
Disabled at:           03:20:37Z
Re-enabled at:         03:20:48Z
Expected next poll:    03:21:04.758Z (Tlast + 90s)
Actual next poll:      03:21:04.940Z ✅
```

### Journal Evidence
```json
{"ts": "2026-05-16T03:19:34.757999+00:00", "level": "INFO", "logger": "central.adapters.nws", "msg": "NWS yielded events", "count": 16}
{"ts": "2026-05-16T03:20:37.616723+00:00", "level": "INFO", "logger": "central.supervisor", "msg": "Adapter stopped", "adapter": "nws", "preserved_last_poll": "2026-05-16T03:19:34.758524+00:00"}
{"ts": "2026-05-16T03:20:48.947358+00:00", "level": "INFO", "logger": "central.supervisor", "msg": "Adapter restarted", "adapter": "nws", "cadence_s": 90, "preserved_last_poll": "2026-05-16T03:19:34.758524+00:00", "next_poll": "2026-05-16T03:21:04.758524+00:00"}
{"ts": "2026-05-16T03:21:04.940891+00:00", "level": "INFO", "logger": "central.adapters.nws", "msg": "NWS poll completed", "status": 200, "feature_count": 354}
```

**Key observations:**
- `preserved_last_poll` appears in BOTH stop and restart logs (proves state retained)
- `next_poll` calculated from `preserved_last_poll + cadence_s` (not from current time)
- Poll did NOT happen immediately on re-enable

**Result:** Rate-limit guarantee upheld through enable/disable cycle. ✅

---

## Phase B Step 5: T1 Capture and Soak

**T1 Timestamp:** 2026-05-16T03:23:19Z
**T2 Timestamp:** 2026-05-16T03:33:48Z

### T1 State
- Upstream alerts: 16
- DB events: 30

### T2 State (after 10-minute soak)
- Upstream alerts: 16
- DB events: 30

### Poll Activity During Soak
```
03:24:05 - NWS poll completed, status: 200, feature_count: 355
03:25:35 - NWS poll completed, status: 200, feature_count: 357
03:27:05 - NWS poll completed, status: 200, feature_count: 358
03:28:35 - NWS poll completed, status: 200, feature_count: 360
03:30:05 - NWS poll completed, status: 200, feature_count: 357
03:31:35 - NWS poll completed, status: 200, feature_count: 356
03:33:05 - NWS poll completed, status: 200, feature_count: 355
```

**Errors during soak:** None ✅

---

## Phase B Step 6: Data Integrity Check

### Verification
```
Upstream alerts: 16
DB events (total): 30
Missing from DB: 0
All upstream alerts found in DB ✓
```

**Result:** Zero missed alerts. Data integrity confirmed. ✅

---

## Phase B Verification Summary

| Step | Test | Result |
|------|------|--------|
| 2 | Config source cutover | ✅ "Config source: db" in logs |
| 3 | Cadence hot-reload | ✅ Poll at Tlast + new_cadence |
| 4 | Enable/disable cycle | ✅ Rate-limit preserved |
| 5 | 10-minute soak | ✅ No errors |
| 6 | Data integrity | ✅ All alerts in DB |

**Phase B Complete.** System running stable on DB-backed config.
