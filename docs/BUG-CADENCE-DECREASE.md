# Bug Investigation: Cadence Decrease Hot-Reload

**Date:** 2026-05-16
**Component:** central-supervisor
**File:** `supervisor.py`

---

## 1. Reproduction

### Test Case: Decrease 60s → 30s
```
Tlast (poll completed): 04:18:24Z
Config change applied:  04:18:30Z (approx)
Expected next poll:     04:18:54Z (Tlast + 30s)
Actual next poll:       04:19:24Z (Tlast + 60s - OLD cadence)
Subsequent polls:       Also at 60s intervals
```

### Log Evidence
```json
{"ts": "...", "msg": "Rescheduled adapter", "adapter": "nws", "old_cadence_s": 60, "new_cadence_s": 30, "next_poll": "2026-05-16T04:18:54+00:00"}
```
- "Rescheduled adapter" log fires with **correct** calculated next_poll
- Actual poll occurs at OLD cadence time
- Subsequent polls continue at OLD cadence

### Contrast: Increase 60s → 90s (WORKS)
```
Tlast:              03:16:34Z
Config change:      03:16:36Z
Expected next poll: 03:18:04Z (Tlast + 90s)
Actual next poll:   03:18:04Z ✅
```

---

## 2. Root Cause

### Location
`supervisor.py` lines 395-450 (`_reschedule_adapter`) and lines 144-181 (`_run_adapter_loop`)

### The Bug
The `cancel_event.set()` call in `_reschedule_adapter` does not reliably wake the `asyncio.wait_for()` in the adapter loop when the cadence is **decreased**.

### Why It Happens

1. **Event handler holds lock during signal:**
   ```python
   # _on_config_change (line 466)
   async with self._lock:
       new_config = await self._config_source.get_adapter(adapter_name)
       # ...
       await self._reschedule_adapter(adapter_name, new_config)  # sets cancel_event here
   ```

2. **Reschedule updates config then signals:**
   ```python
   # _reschedule_adapter
   state.config = new_config     # Line 420
   state.adapter.cadence_s = new_cadence  # Line 423
   # ... logging ...
   state.cancel_event.set()      # Line 450 - inside lock context
   ```

3. **Asyncio event delivery delay:**
   The `asyncio.Event.set()` queues a wakeup for waiting tasks, but the signal delivery is subject to asyncio's task scheduler. When called from within an `async with` block, the event may not be processed until the current task yields or the lock context exits.

4. **Timing difference between increase and decrease:**
   - **Increase (60→90):** Loop has ~30-50s remaining sleep. Event signal arrives well before timeout.
   - **Decrease (90→60):** Loop may be ~10s from timeout. By the time event signal is processed, timeout has already fired.

5. **Why subsequent polls use old cadence:**
   When the loop times out naturally (rather than being woken by event), it proceeds to poll. After poll completes, `state.last_completed_poll` is updated. The loop then reads `state.config.cadence_s` for the NEXT iteration — but if `state.config` was somehow not durably updated (or there's a stale reference), it uses the old value.

   **Alternative theory:** The `state.config = new_config` assignment creates a new config object, but the loop may be reading from a captured reference to the old object if there's any closure behavior we're not seeing.

---

## 3. Proposed Fix

### Option A: Force immediate reschedule (Recommended)

Move the cancel logic OUTSIDE the lock, and use a more aggressive wake pattern:

```python
async def _reschedule_adapter(self, name: str, new_config: AdapterConfig) -> None:
    state = self._adapter_states.get(name)
    if state is None or not state.is_running:
        await self._start_adapter(new_config)
        return

    old_cadence = state.config.cadence_s
    new_cadence = new_config.cadence_s

    # Update config atomically
    state.config = new_config
    state.adapter.cadence_s = new_cadence

    # ... (NWS-specific updates, logging) ...

    # Cancel and wait for acknowledgment
    state.cancel_event.set()
    await asyncio.sleep(0)  # Force task switch to process event
```

### Option B: Stop and restart the loop task

For cadence changes, stop the current loop task and create a new one:

```python
async def _reschedule_adapter(self, name: str, new_config: AdapterConfig) -> None:
    state = self._adapter_states.get(name)
    if state is None:
        await self._start_adapter(new_config)
        return

    # Preserve last_completed_poll
    preserved_poll = state.last_completed_poll

    # Stop current loop
    await self._stop_adapter(name)

    # Update config
    state.config = new_config
    state.last_completed_poll = preserved_poll

    # Restart loop
    await self._start_adapter(new_config)
```

### Option C: Double-signal pattern

Set the event, yield, then set again to ensure delivery:

```python
state.cancel_event.set()
await asyncio.sleep(0)
state.cancel_event.set()  # Redundant but ensures visibility
```

---

## 4. Test Gap

### Missing Tests

The test file `test_config_source_new.py` only tests ConfigSource behavior (list, get, protocol compliance). There are **no tests** for:

1. `_reschedule_adapter` interrupting a sleeping loop
2. Cadence decrease being applied mid-sleep
3. Cadence increase being applied mid-sleep
4. Rate-limit guarantee after reschedule
5. `cancel_event` mechanism in isolation

### Recommended Tests

```python
@pytest.mark.asyncio
async def test_cadence_decrease_applies_immediately():
    """Cadence decrease should wake sleeping loop and reschedule."""
    # Setup: Adapter polling at 60s cadence
    # Action: Change cadence to 30s while sleeping
    # Assert: Next poll at last_poll + 30s, not last_poll + 60s

@pytest.mark.asyncio
async def test_cadence_increase_applies_on_next_cycle():
    """Cadence increase should wake sleeping loop and extend wait."""
    # Setup: Adapter polling at 60s cadence
    # Action: Change cadence to 90s while sleeping
    # Assert: Next poll at last_poll + 90s

@pytest.mark.asyncio
async def test_cancel_event_wakes_sleeping_loop():
    """cancel_event.set() should interrupt asyncio.wait_for()."""
    # Unit test for the event mechanism in isolation
```

---

## 5. State at End

### LXC State (Reverted)
- **Cadence in DB:** 60s ✅
- **Actual poll interval:** 60s ✅
- **Supervisor restarted:** 2026-05-16T04:43:40Z
- **Verified polls:**
  ```
  04:43:40.964 - First poll after restart
  04:44:41.171 - Second poll (61s later) ✅
  ```

### Mitigation Until Fix
After any cadence change (especially decrease), verify actual poll intervals. If incorrect, restart supervisor:
```bash
systemctl restart central-supervisor
```

---

## Summary

| Item | Details |
|------|---------|
| **Bug** | Cadence decrease hot-reload doesn't apply without restart |
| **Root cause** | `cancel_event.set()` inside lock context has delayed delivery |
| **Affects** | Cadence decreases only; increases work correctly |
| **Workaround** | Restart supervisor after cadence decrease |
| **Fix effort** | Low - add `await asyncio.sleep(0)` after event.set() |
| **Test coverage** | None for hot-reload mechanism |

