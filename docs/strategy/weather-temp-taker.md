# Weather Temperature Taker Strategy

## Scope

This v1 strategy handles weather threshold markets that can be mapped to a single official NWS station and priced from structured forecast plus observation data.

## Inputs

- Kalshi market snapshot with fixed-point `*_dollars` fields
- NWS forecast data for the mapped point
- Latest NWS station observation

## Fair value model

1. Extract the daytime high forecast.
2. Compare it to the market threshold.
3. Convert the threshold gap to a probability with a logistic curve.
4. Boost confidence when the latest observation already strongly supports the outcome.

The current implementation is intentionally simple and interpretable. It is meant to be replaced or extended once live shadow data shows where it is weak.

## Taker logic

- Buy YES when `fair_yes - yes_ask >= min_edge`.
- Buy NO when `fair_no - no_ask >= min_edge`.
- Otherwise stand down.

Order size is clipped by:

- max order count
- max order notional
- confidence scaling

## Exclusions

- market making
- news or social-data driven trading
- unsupported settlement sources
- unmapped weather markets

