"""Central CLI commands."""

import argparse
import asyncio
import sys


async def config_store_check() -> int:
    """Smoke test for config store connectivity.

    Connects via bootstrap_config, lists adapters, and verifies crypto.
    Returns 0 on success, 1 on failure.
    """
    from central.bootstrap_config import get_settings
    from central.config_store import ConfigStore
    from central.crypto import decrypt, encrypt

    settings = get_settings()
    print(f"Connecting to: {settings.db_dsn.split('@')[1]}")  # Hide password

    try:
        store = await ConfigStore.create(settings.db_dsn)
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        return 1

    try:
        # List adapters
        adapters = await store.list_adapters()
        print(f"\nAdapters ({len(adapters)}):")
        for adapter in adapters:
            print(f"  - {adapter.name}: enabled={adapter.enabled}, cadence_s={adapter.cadence_s}")
            print(f"    settings: {adapter.settings}")

        # Test crypto
        test_plaintext = b"config_store_check_test"
        try:
            ciphertext = encrypt(test_plaintext)
            decrypted = decrypt(ciphertext)
            if decrypted == test_plaintext:
                print("\ncrypto: ok")
            else:
                print("\ncrypto: FAILED (round-trip mismatch)")
                return 1
        except Exception as e:
            print(f"\ncrypto: FAILED ({e})")
            return 1

        print("\nAll checks passed.")
        return 0

    finally:
        await store.close()


def main_config_store_check() -> None:
    """Entry point for central-cli config-store-check."""
    sys.exit(asyncio.run(config_store_check()))


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Central CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("config-store-check", help="Test config store connectivity")

    args = parser.parse_args()

    if args.command == "config-store-check":
        main_config_store_check()


if __name__ == "__main__":
    main()
