# Phase 1B Planning Notes

Design notes for Phase 1B GUI features. These are planning items, not
implementation specifications.

## Stream Retention GUI

### Per-Stream Configuration
- Show each stream from `config.streams` table
- Editable max_age_s with preset chips: 1d, 7d, 14d, 30d, 365d
- Custom numeric input allowed (operator can enter 90d, etc.)
- Changes trigger NATS stream update via supervisor hot-reload

### Storage Monitor
Per stream, display:
- **Current bytes**: Live from `nats stream info`
- **Projected bytes**: Calculated from current rate × max_age
- **Days remaining**: Current_bytes / rate_per_day estimate
- Refresh: Real-time polling, not cached

### Global Server Cap
- Show `max_file_store` value as read-only reference
- Editing requires NATS server restart (out of scope for GUI)
- Display per-stream ceiling (30% of server cap) as context

## Region Picker

### Interactive Map
- Bbox selection via click-drag rectangle
- Same UI component for all adapters (NWS, FIRMS, USGS)
- Stores `{north, south, east, west}` floats
- Preview of coverage area with state/country boundaries

### Preset Regions
- Common presets: CONUS, Pacific Northwest, Mountain West
- Quick-select buttons alongside custom draw

## API Key Management

### Key Storage
- View configured API keys (alias only, not values)
- Add new keys with alias and value
- Values encrypted at rest in `config.api_keys`
- Rotation: update value, track `rotated_at`

### Required Keys by Adapter
- **FIRMS** (Phase 1a-6): `MAP_KEY` for NASA FIRMS API
- Future adapters may require additional keys

## Technical Notes

- All GUI changes write to `config.*` tables
- Supervisor receives NOTIFY and hot-reloads
- No service restarts required for config changes
- Stream retention changes apply within 5 seconds

## FIRMS Adapter Configuration

### MAP_KEY Management
- Display key alias () and  timestamp
- Allow operator to rotate key value (re-encrypt new key)
- Show warning if key not present (polling disabled)
- No key value display (security)

### Satellite Selection
- Toggle individual satellites: VIIRS_SNPP, VIIRS_NOAA20, VIIRS_NOAA21
- Stored in  array
- Changes hot-reload to adapter without restart

### SNPP End-of-Life Notice
- NASA timeline: SNPP mission ends ~October 2026
- GUI should display warning banner when SNPP is enabled and date approaches
- Recommend adding NOAA-21 to satellites list before SNPP EOL
- After EOL, adapter will fail to fetch SNPP data (404); GUI should surface this

## FIRMS Adapter Configuration

### MAP_KEY Management
- Display key alias (firms) and last_used_at timestamp
- Allow operator to rotate key value (re-encrypt new key)
- Show warning if key not present (polling disabled)
- No key value display (security)

### Satellite Selection
- Toggle individual satellites: VIIRS_SNPP, VIIRS_NOAA20, VIIRS_NOAA21
- Stored in config.adapters.settings.satellites array
- Changes hot-reload to adapter without restart

### SNPP End-of-Life Notice
- NASA timeline: SNPP mission ends ~October 2026
- GUI should display warning banner when SNPP is enabled and date approaches
- Recommend adding NOAA-21 to satellites list before SNPP EOL
- After EOL, adapter will fail to fetch SNPP data (404); GUI should surface this

## USGS Quake Adapter Configuration

### Feed Selection
- Default: all_hour (updated every minute, low latency)
- Options: all_hour, all_day, all_week, all_month
- Operators can switch to all_day for less frequent polling with broader window
- Stored in config.adapters.settings.feed

### Magnitude Tier Color Coding
For GUI display of earthquake events:
- **minor** (M < 3.0): Gray
- **light** (3.0-3.9): Yellow
- **moderate** (4.0-4.9): Orange
- **strong** (5.0-5.9): Red
- **major** (6.0-6.9): Dark Red
- **great** (M >= 7.0): Purple

Colors follow USGS conventions for earthquake hazard communication.
