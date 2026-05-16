"""Tests for cryptographic primitives."""

import base64
import os
from pathlib import Path

import pytest

from central.crypto import (
    KEY_SIZE,
    DecryptionError,
    KeyLoadError,
    clear_key_cache,
    decrypt,
    encrypt,
)


@pytest.fixture
def master_key(tmp_path: Path) -> Path:
    """Create a valid master key file."""
    key = os.urandom(KEY_SIZE)
    key_path = tmp_path / "master.key"
    key_path.write_text(base64.b64encode(key).decode())
    clear_key_cache()
    return key_path


@pytest.fixture
def wrong_key(tmp_path: Path) -> Path:
    """Create a different master key file."""
    key = os.urandom(KEY_SIZE)
    key_path = tmp_path / "wrong.key"
    key_path.write_text(base64.b64encode(key).decode())
    return key_path


class TestEncryptDecrypt:
    """Test encrypt/decrypt round-trip."""

    def test_round_trip(self, master_key: Path) -> None:
        """Encrypting then decrypting returns original plaintext."""
        plaintext = b"Hello, Central!"

        ciphertext = encrypt(plaintext, key_path=master_key)
        decrypted = decrypt(ciphertext, key_path=master_key)

        assert decrypted == plaintext

    def test_round_trip_empty(self, master_key: Path) -> None:
        """Empty plaintext encrypts and decrypts correctly."""
        plaintext = b""

        ciphertext = encrypt(plaintext, key_path=master_key)
        decrypted = decrypt(ciphertext, key_path=master_key)

        assert decrypted == plaintext

    def test_round_trip_large(self, master_key: Path) -> None:
        """Large plaintext encrypts and decrypts correctly."""
        plaintext = os.urandom(1024 * 1024)  # 1MB

        ciphertext = encrypt(plaintext, key_path=master_key)
        decrypted = decrypt(ciphertext, key_path=master_key)

        assert decrypted == plaintext

    def test_ciphertext_different_each_time(self, master_key: Path) -> None:
        """Same plaintext produces different ciphertext (random nonce)."""
        plaintext = b"test"

        ct1 = encrypt(plaintext, key_path=master_key)
        ct2 = encrypt(plaintext, key_path=master_key)

        assert ct1 != ct2
        # But both decrypt to same plaintext
        assert decrypt(ct1, key_path=master_key) == plaintext
        assert decrypt(ct2, key_path=master_key) == plaintext


class TestDecryptionFailures:
    """Test AEAD authentication catches tampering."""

    def test_wrong_key_fails(self, master_key: Path, wrong_key: Path) -> None:
        """Decryption with wrong key raises DecryptionError."""
        plaintext = b"secret"
        ciphertext = encrypt(plaintext, key_path=master_key)

        clear_key_cache()  # Clear cache so wrong_key is loaded
        with pytest.raises(DecryptionError):
            decrypt(ciphertext, key_path=wrong_key)

    def test_tampered_ciphertext_fails(self, master_key: Path) -> None:
        """Modified ciphertext is detected and rejected."""
        plaintext = b"secret"
        ciphertext = encrypt(plaintext, key_path=master_key)

        # Flip a bit in the ciphertext (after nonce, before tag)
        tampered = bytearray(ciphertext)
        tampered[15] ^= 0x01  # Flip one bit
        tampered = bytes(tampered)

        with pytest.raises(DecryptionError):
            decrypt(tampered, key_path=master_key)

    def test_tampered_tag_fails(self, master_key: Path) -> None:
        """Modified authentication tag is detected and rejected."""
        plaintext = b"secret"
        ciphertext = encrypt(plaintext, key_path=master_key)

        # Flip a bit in the last byte (part of the tag)
        tampered = bytearray(ciphertext)
        tampered[-1] ^= 0x01
        tampered = bytes(tampered)

        with pytest.raises(DecryptionError):
            decrypt(tampered, key_path=master_key)

    def test_truncated_ciphertext_fails(self, master_key: Path) -> None:
        """Truncated ciphertext is rejected."""
        ciphertext = b"tooshort"

        with pytest.raises(DecryptionError, match="too short"):
            decrypt(ciphertext, key_path=master_key)


class TestKeyLoading:
    """Test master key loading."""

    def test_missing_key_file(self, tmp_path: Path) -> None:
        """Missing key file raises KeyLoadError."""
        clear_key_cache()
        missing = tmp_path / "nonexistent.key"

        with pytest.raises(KeyLoadError, match="not found"):
            encrypt(b"test", key_path=missing)

    def test_invalid_key_size(self, tmp_path: Path) -> None:
        """Key file with wrong size raises KeyLoadError."""
        clear_key_cache()
        bad_key = tmp_path / "bad.key"
        bad_key.write_text(base64.b64encode(b"tooshort").decode())

        with pytest.raises(KeyLoadError, match="Invalid master key size"):
            encrypt(b"test", key_path=bad_key)

    def test_invalid_base64(self, tmp_path: Path) -> None:
        """Invalid base64 in key file raises KeyLoadError."""
        clear_key_cache()
        bad_key = tmp_path / "bad.key"
        bad_key.write_text("not valid base64!!!")

        with pytest.raises(KeyLoadError):
            encrypt(b"test", key_path=bad_key)

    def test_key_cached(self, master_key: Path) -> None:
        """Key is cached after first load."""
        # First encryption loads the key
        encrypt(b"test1", key_path=master_key)

        # Delete the file
        master_key.unlink()

        # Second encryption should still work (cached)
        ciphertext = encrypt(b"test2", key_path=master_key)
        assert len(ciphertext) > 0

    def test_cache_clear(self, master_key: Path) -> None:
        """clear_key_cache forces reload."""
        encrypt(b"test", key_path=master_key)
        master_key.unlink()
        clear_key_cache()

        with pytest.raises(KeyLoadError, match="not found"):
            encrypt(b"test", key_path=master_key)
