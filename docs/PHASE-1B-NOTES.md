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
