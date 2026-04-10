# Security and Key Handling

## Secrets

- Do not commit `.env`, PEM files, or downloaded Kalshi key exports.
- Mount private keys as read-only files in production.
- Prefer separate read and write keys.

## Live execution guardrails

- Keep `APP_SHADOW_MODE=true` until reconciliation is stable.
- Use the global kill switch during deploys and incidents.
- Use a dedicated Kalshi subaccount for the bot.

## VPS hardening

- SSH keys only
- UFW enabled
- reverse proxy only exposes the web UI
- Postgres should not be publicly reachable
- keep backups encrypted

## Credential rotation

1. Create a new Kalshi key pair.
2. Mount the new PEM file on both blue and green deployments.
3. Update environment variables for the inactive color first.
4. Promote the inactive color.
5. Rotate the old color.
6. Revoke the old key.

