"""Cryptographic primitives for secret storage.

Uses AES-256-GCM for authenticated encryption. The master key is read
from the path specified in bootstrap config on first use and cached.
"""

import base64
import os
from functools import lru_cache
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# AES-256 requires 32-byte key
KEY_SIZE = 32
# GCM nonce size (96 bits recommended by NIST)
NONCE_SIZE = 12


class CryptoError(Exception):
    """Base exception for crypto operations."""


class KeyLoadError(CryptoError):
    """Failed to load master key."""


class DecryptionError(CryptoError):
    """Failed to decrypt ciphertext (wrong key or tampered data)."""


@lru_cache
def _load_master_key(path: Path) -> bytes:
    """Load and decode the base64-encoded master key from file."""
    try:
        key_b64 = path.read_text().strip()
        key = base64.b64decode(key_b64)
    except FileNotFoundError:
        raise KeyLoadError(f"Master key file not found: {path}")
    except Exception as e:
        raise KeyLoadError(f"Failed to read master key from {path}: {e}")

    if len(key) != KEY_SIZE:
        raise KeyLoadError(
            f"Invalid master key size: expected {KEY_SIZE} bytes, got {len(key)}"
        )
    return key


def encrypt(plaintext: bytes, key_path: Path | None = None) -> bytes:
    """Encrypt plaintext using AES-256-GCM.

    Args:
        plaintext: Data to encrypt.
        key_path: Path to master key file. If None, uses default from
            bootstrap config.

    Returns:
        Ciphertext in format: nonce (12 bytes) || ciphertext || tag (16 bytes)
    """
    if key_path is None:
        from central.bootstrap_config import get_settings
        key_path = get_settings().master_key_path

    key = _load_master_key(key_path)
    nonce = os.urandom(NONCE_SIZE)
    aesgcm = AESGCM(key)

    # GCM appends the 16-byte tag to the ciphertext
    ciphertext_with_tag = aesgcm.encrypt(nonce, plaintext, associated_data=None)

    return nonce + ciphertext_with_tag


def decrypt(ciphertext: bytes, key_path: Path | None = None) -> bytes:
    """Decrypt ciphertext using AES-256-GCM.

    Args:
        ciphertext: Data in format: nonce || ciphertext || tag
        key_path: Path to master key file. If None, uses default from
            bootstrap config.

    Returns:
        Decrypted plaintext.

    Raises:
        DecryptionError: If decryption fails (wrong key or tampered data).
    """
    if key_path is None:
        from central.bootstrap_config import get_settings
        key_path = get_settings().master_key_path

    if len(ciphertext) < NONCE_SIZE + 16:  # nonce + minimum tag
        raise DecryptionError("Ciphertext too short")

    key = _load_master_key(key_path)
    nonce = ciphertext[:NONCE_SIZE]
    ciphertext_with_tag = ciphertext[NONCE_SIZE:]

    aesgcm = AESGCM(key)
    try:
        plaintext = aesgcm.decrypt(nonce, ciphertext_with_tag, associated_data=None)
    except Exception as e:
        raise DecryptionError(f"Decryption failed: {e}")

    return plaintext


def clear_key_cache() -> None:
    """Clear the cached master key. Use after key rotation."""
    _load_master_key.cache_clear()
