"""
Integration test for CSRF race condition fix.

This test verifies that the session-bound CSRF implementation fixes the race
condition where interleaved GET requests would invalidate CSRF tokens.

See: PR #24 - Central 1b-8 fix-up phase 2
"""

import pytest


class TestCsrfRaceConditionFix:
    """Verify that interleaved GETs don't break CSRF validation."""

    def test_session_bound_csrf_consistent_across_gets(self):
        """Session-bound CSRF tokens remain consistent across multiple GETs.

        This was the core bug: fastapi-csrf-protect rotated tokens on every GET,
        causing race conditions when users had multiple tabs or slow connections.
        
        With session-bound CSRF, the token is stored in the session row and
        remains constant until the session is destroyed.
        """
        from unittest.mock import MagicMock, AsyncMock
        from central.gui.auth import get_session
        
        # Mock a session with a csrf_token
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={
            "id": 1,
            "username": "testuser",
            "created_at": "2024-01-01T00:00:00Z",
            "password_changed_at": "2024-01-01T00:00:00Z",
            "csrf_token": "fixed_csrf_token_12345",
        })
        
        import asyncio
        
        async def test():
            # First GET
            result1 = await get_session(mock_conn, "test-token")
            assert result1 is not None
            op1, csrf1 = result1
            
            # Second GET (simulating interleaved request)
            result2 = await get_session(mock_conn, "test-token")
            assert result2 is not None
            op2, csrf2 = result2
            
            # CSRF tokens should be identical (the fix!)
            assert csrf1 == csrf2 == "fixed_csrf_token_12345"
        
        asyncio.run(test())

    def test_pre_auth_csrf_tokens_independently_valid(self):
        """Pre-auth CSRF tokens are independently valid.
        
        For unauthenticated routes, each GET generates a new token+cookie pair.
        Each pair should validate independently, allowing the original token
        to work even if another GET happened in between.
        """
        from central.gui.csrf import generate_pre_auth_csrf, validate_pre_auth_csrf
        from unittest.mock import MagicMock
        
        secret = "testsecret12345678901234567890ab"
        
        # First GET generates token1 + cookie1
        token1, signed1 = generate_pre_auth_csrf(secret)
        
        # Second GET generates token2 + cookie2  
        token2, signed2 = generate_pre_auth_csrf(secret)
        
        # Tokens should be different (fresh random tokens)
        assert token1 != token2
        assert signed1 != signed2
        
        # But each pair should validate independently
        mock_request1 = MagicMock()
        mock_request1.cookies = {"central_preauth_csrf": signed1}
        
        mock_request2 = MagicMock()
        mock_request2.cookies = {"central_preauth_csrf": signed2}
        
        # Original token still validates with original cookie
        assert validate_pre_auth_csrf(mock_request1, token1, secret) is True
        
        # Second token validates with second cookie
        assert validate_pre_auth_csrf(mock_request2, token2, secret) is True
        
        # Cross-validation should fail
        assert validate_pre_auth_csrf(mock_request1, token2, secret) is False
        assert validate_pre_auth_csrf(mock_request2, token1, secret) is False

    def test_csrf_token_generation_is_secure(self):
        """CSRF tokens are cryptographically secure."""
        from central.gui.auth import generate_csrf_token
        
        # Generate multiple tokens
        tokens = [generate_csrf_token() for _ in range(100)]
        
        # All tokens should be unique
        assert len(set(tokens)) == 100
        
        # Tokens should be 64 hex chars (32 bytes)
        for token in tokens:
            assert len(token) == 64
            assert all(c in "0123456789abcdef" for c in token)
