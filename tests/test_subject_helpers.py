"""Tests for the _subject_helpers module (v0.9.20).

Tests cover:
- US_STATE_NAME_TO_CODE mapping coverage
- subject_for_country normalization
- subject_for_region US/intl/unknown paths
"""


from central.adapters._subject_helpers import (
    US_STATE_NAME_TO_CODE,
    subject_for_country,
    subject_for_region,
    _state_name_to_code,
)


class TestUSStateNameToCode:
    """US state name mapping tests."""

    def test_all_50_states_present(self):
        """All 50 states should be in the mapping."""
        # Count unique 2-letter codes (excluding DC and territories)
        state_codes = {v for k, v in US_STATE_NAME_TO_CODE.items()
                       if v not in ('dc', 'pr', 'vi', 'gu', 'as', 'mp')}
        assert len(state_codes) == 50

    def test_dc_present(self):
        assert US_STATE_NAME_TO_CODE.get('district of columbia') == 'dc'
        assert US_STATE_NAME_TO_CODE.get('washington dc') == 'dc'

    def test_territories_present(self):
        assert US_STATE_NAME_TO_CODE.get('puerto rico') == 'pr'
        assert US_STATE_NAME_TO_CODE.get('guam') == 'gu'

    def test_idaho(self):
        assert US_STATE_NAME_TO_CODE.get('idaho') == 'id'

    def test_nevada(self):
        assert US_STATE_NAME_TO_CODE.get('nevada') == 'nv'


class TestStateNameToCode:
    """_state_name_to_code helper tests."""

    def test_full_name_lookup(self):
        assert _state_name_to_code('Idaho') == 'id'
        assert _state_name_to_code('UTAH') == 'ut'
        assert _state_name_to_code('new york') == 'ny'

    def test_already_2char_code(self):
        """Some enrichers return 2-char codes, pass through."""
        assert _state_name_to_code('ID') == 'id'
        assert _state_name_to_code('nv') == 'nv'

    def test_whitespace_handling(self):
        assert _state_name_to_code('  Idaho  ') == 'id'

    def test_unknown_name_returns_none(self):
        assert _state_name_to_code('Narnia') is None
        assert _state_name_to_code('') is None
        assert _state_name_to_code(None) is None


class TestSubjectForCountry:
    """subject_for_country normalization tests."""

    def test_basic_country(self):
        assert subject_for_country('Japan') == 'japan'
        assert subject_for_country('Mexico') == 'mexico'

    def test_multi_word_hyphenated(self):
        assert subject_for_country('United States') == 'united-states'
        assert subject_for_country('New Zealand') == 'new-zealand'

    def test_comma_separated_takes_first(self):
        """Photon may return multiple values."""
        assert subject_for_country('Japan, Asia') == 'japan'

    def test_empty_returns_unknown(self):
        assert subject_for_country('') == 'unknown'
        assert subject_for_country(None) == 'unknown'
        assert subject_for_country('   ') == 'unknown'


class TestSubjectForRegion:
    """subject_for_region integration tests."""

    def test_us_event_with_state(self):
        """US event returns us.<state>."""
        data = {
            '_enriched': {
                'geocoder': {
                    'country': 'United States',
                    'state': 'Idaho',
                }
            }
        }
        assert subject_for_region(data) == 'us.id'

    def test_us_event_nevada(self):
        """Nevada quake (the bug report example)."""
        data = {
            '_enriched': {
                'geocoder': {
                    'country': 'United States',
                    'state': 'Nevada',
                }
            }
        }
        assert subject_for_region(data) == 'us.nv'

    def test_us_event_with_2char_state(self):
        """Some enrichers return 2-char codes."""
        data = {
            '_enriched': {
                'geocoder': {
                    'country': 'United States',
                    'state': 'ID',
                }
            }
        }
        assert subject_for_region(data) == 'us.id'

    def test_us_event_no_state(self):
        """US event with no state returns us.unknown (known US, unresolved state)."""
        data = {
            '_enriched': {
                'geocoder': {
                    'country': 'United States',
                    'state': None,
                }
            }
        }
        assert subject_for_region(data) == 'us.unknown'

    def test_intl_event_japan(self):
        """International event returns country token."""
        data = {
            '_enriched': {
                'geocoder': {
                    'country': 'Japan',
                    'state': 'Tokyo',  # Ignored for intl
                }
            }
        }
        assert subject_for_region(data) == 'japan'

    def test_intl_event_multi_word(self):
        data = {
            '_enriched': {
                'geocoder': {
                    'country': 'New Zealand',
                }
            }
        }
        assert subject_for_region(data) == 'new-zealand'

    def test_no_enriched_data(self):
        """Missing enrichment returns unknown."""
        assert subject_for_region({}) == 'unknown'
        assert subject_for_region({'_enriched': None}) == 'unknown'
        assert subject_for_region({'_enriched': {}}) == 'unknown'

    def test_no_geocoder(self):
        data = {'_enriched': {'other': 'stuff'}}
        assert subject_for_region(data) == 'unknown'

    def test_empty_country(self):
        data = {
            '_enriched': {
                'geocoder': {
                    'country': '',
                }
            }
        }
        assert subject_for_region(data) == 'unknown'
