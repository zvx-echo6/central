# Central Tests

## Test Database

Some tests (notably `test_config_store.py`) require a real PostgreSQL database.
By default, tests connect to:

```
postgresql://central_test:testpass@localhost/central_test
```

If your test database uses different credentials, set the `CENTRAL_TEST_DB_DSN`
environment variable:

```bash
export CENTRAL_TEST_DB_DSN="postgresql://myuser:mypass@localhost/mydb"
uv run pytest tests/test_config_store.py
```
