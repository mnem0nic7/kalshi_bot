# Runbooks

## Stale weather feed

1. Leave the kill switch enabled.
2. Confirm NWS API availability.
3. Check whether the issue is point lookup, forecast fetch, or observation fetch.
4. Resume only after fresh weather data lands and the next room run shows healthy timestamps.

## Kalshi auth or signing failure

1. Confirm the key ID and PEM path.
2. Confirm the signed path excludes query parameters.
3. Validate the container-mounted key file permissions.
4. Rotate the key if the signature still fails.

## Duplicate-order suspicion

1. Enable the kill switch.
2. Query `orders` by `client_order_id`.
3. Compare against Kalshi `GET /portfolio/orders`.
4. Resume only after reconciliation is clean.

## Blue/green rollback

1. Enable the kill switch.
2. Promote the previously healthy color.
3. Confirm the new active color reacquires the execution lock.
4. Disable the kill switch when stable.

