# Security Policy

Do not commit credentials, `.env` files, database dumps, runtime logs, private keys, PEM files, or production server details.

Before publishing to GitHub, run:

```bash
grep -R "password\|secret\|pem\|PRIVATE KEY\|DB_PASSWORD" -n . --exclude-dir=.git
```

Use environment variables or a secret manager for production deployments.
