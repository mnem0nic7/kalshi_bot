from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from kalshi_bot.weather.models import WeatherMarketMapping

EULER_GAMMA = 0.5772156649015329
MIN_SIGMA_F = 0.25
MIN_KDE_BANDWIDTH_F = 0.25


def _clamp_probability(value: float) -> float:
    if not math.isfinite(value):
        raise ValueError(f"probability is not finite: {value!r}")
    return max(0.0, min(1.0, value))


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


@dataclass(frozen=True, slots=True)
class TemperatureBucket:
    """Temperature interval for a YES payout.

    `None` means an open bound. Continuous distributions make strict vs.
    inclusive threshold semantics equivalent for probability purposes.
    """

    low_f: float | None = None
    high_f: float | None = None

    def __post_init__(self) -> None:
        if self.low_f is not None and not math.isfinite(self.low_f):
            raise ValueError("bucket low_f must be finite or None")
        if self.high_f is not None and not math.isfinite(self.high_f):
            raise ValueError("bucket high_f must be finite or None")
        if self.low_f is not None and self.high_f is not None and self.low_f >= self.high_f:
            raise ValueError("bucket low_f must be below high_f")

    @classmethod
    def for_mapping(cls, mapping: WeatherMarketMapping) -> "TemperatureBucket":
        if mapping.threshold_f is None:
            raise ValueError(f"{mapping.market_ticker} has no threshold_f")
        if mapping.operator in (">", ">="):
            return cls(low_f=float(mapping.threshold_f), high_f=None)
        return cls(low_f=None, high_f=float(mapping.threshold_f))

    def to_dict(self) -> dict[str, float | None]:
        return {"low_f": self.low_f, "high_f": self.high_f}


@dataclass(frozen=True, slots=True)
class GumbelFit:
    mean_f: float
    sigma_f: float
    mu_f: float
    beta_f: float

    def to_dict(self) -> dict[str, float]:
        return {
            "mean_f": self.mean_f,
            "sigma_f": self.sigma_f,
            "mu_f": self.mu_f,
            "beta_f": self.beta_f,
        }


@dataclass(frozen=True, slots=True)
class ProbabilityEngineConfig:
    pseudo_count: float = 8.0
    boundary_delta_f: float = 1.0
    kde_min_sources: int = 2
    kde_min_members: int = 30
    kde_blend_weight: float = 0.5

    def __post_init__(self) -> None:
        if self.pseudo_count < 0:
            raise ValueError("pseudo_count must be non-negative")
        if self.boundary_delta_f <= 0:
            raise ValueError("boundary_delta_f must be positive")
        if self.kde_min_sources < 1:
            raise ValueError("kde_min_sources must be positive")
        if self.kde_min_members < 1:
            raise ValueError("kde_min_members must be positive")
        if not 0 <= self.kde_blend_weight <= 1:
            raise ValueError("kde_blend_weight must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class ProbabilityEstimate:
    p_bucket_yes: float
    p_gumbel_yes: float
    p_kde_yes: float | None
    p_model_yes: float
    p_climo_yes: float
    shrinkage_alpha: float
    boundary_mass: float
    disagreement: float
    effective_member_count: int
    source_set_used: list[str]
    gumbel_fit: GumbelFit
    trace: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "p_bucket_yes": self.p_bucket_yes,
            "p_gumbel_yes": self.p_gumbel_yes,
            "p_kde_yes": self.p_kde_yes,
            "p_model_yes": self.p_model_yes,
            "p_climo_yes": self.p_climo_yes,
            "shrinkage_alpha": self.shrinkage_alpha,
            "boundary_mass": self.boundary_mass,
            "disagreement": self.disagreement,
            "effective_member_count": self.effective_member_count,
            "source_set_used": list(self.source_set_used),
            "gumbel_fit": self.gumbel_fit.to_dict(),
            "trace": dict(self.trace),
        }


def fit_gumbel_from_mean_sigma(mean_f: float, sigma_f: float) -> GumbelFit:
    if not math.isfinite(mean_f):
        raise ValueError("mean_f must be finite")
    if not math.isfinite(sigma_f) or sigma_f <= 0:
        raise ValueError("sigma_f must be positive and finite")
    sigma = max(float(sigma_f), MIN_SIGMA_F)
    beta = sigma * math.sqrt(6.0) / math.pi
    mu = float(mean_f) - EULER_GAMMA * beta
    return GumbelFit(mean_f=float(mean_f), sigma_f=sigma, mu_f=mu, beta_f=beta)


def gumbel_cdf(x_f: float | None, fit: GumbelFit) -> float:
    if x_f is None:
        raise ValueError("open bounds must be handled by bucket probability")
    z = -(float(x_f) - fit.mu_f) / fit.beta_f
    if z > 700:
        return 0.0
    return _clamp_probability(math.exp(-math.exp(z)))


def gumbel_bucket_probability(bucket: TemperatureBucket, fit: GumbelFit) -> float:
    low_cdf = 0.0 if bucket.low_f is None else gumbel_cdf(bucket.low_f, fit)
    high_cdf = 1.0 if bucket.high_f is None else gumbel_cdf(bucket.high_f, fit)
    return _clamp_probability(high_cdf - low_cdf)


def silverman_bandwidth_f(members_f: Sequence[float]) -> float:
    members = _finite_members(members_f)
    if len(members) < 2:
        return MIN_KDE_BANDWIDTH_F
    sigma = statistics.pstdev(members)
    if sigma <= 0:
        return MIN_KDE_BANDWIDTH_F
    return max(MIN_KDE_BANDWIDTH_F, 1.06 * sigma * (len(members) ** (-1.0 / 5.0)))


def kde_bucket_probability(bucket: TemperatureBucket, members_f: Sequence[float], *, bandwidth_f: float | None = None) -> float:
    members = _finite_members(members_f)
    if not members:
        raise ValueError("KDE requires at least one finite member")
    h = max(float(bandwidth_f if bandwidth_f is not None else silverman_bandwidth_f(members)), MIN_KDE_BANDWIDTH_F)
    total = 0.0
    for value in members:
        low = 0.0 if bucket.low_f is None else _normal_cdf((bucket.low_f - value) / h)
        high = 1.0 if bucket.high_f is None else _normal_cdf((bucket.high_f - value) / h)
        total += high - low
    return _clamp_probability(total / len(members))


def _finite_members(members_f: Sequence[float]) -> list[float]:
    return [float(value) for value in members_f if math.isfinite(float(value))]


def _boundary_intervals(bucket: TemperatureBucket, delta_f: float) -> list[TemperatureBucket]:
    intervals: list[TemperatureBucket] = []
    if bucket.low_f is not None:
        intervals.append(TemperatureBucket(low_f=bucket.low_f - delta_f, high_f=bucket.low_f + delta_f))
    if bucket.high_f is not None:
        intervals.append(TemperatureBucket(low_f=bucket.high_f - delta_f, high_f=bucket.high_f + delta_f))
    return intervals


def _model_boundary_mass(
    *,
    bucket: TemperatureBucket,
    fit: GumbelFit,
    source_members_by_source: Mapping[str, Sequence[float]],
    p_kde_available: bool,
    config: ProbabilityEngineConfig,
) -> float:
    intervals = _boundary_intervals(bucket, config.boundary_delta_f)
    if not intervals:
        return 0.0
    all_members = [value for members in source_members_by_source.values() for value in _finite_members(members)]
    mass = 0.0
    for interval in intervals:
        p_gumbel = gumbel_bucket_probability(interval, fit)
        if p_kde_available and all_members:
            p_kde = kde_bucket_probability(interval, all_members)
            mass += ((1.0 - config.kde_blend_weight) * p_gumbel) + (config.kde_blend_weight * p_kde)
        else:
            mass += p_gumbel
    return _clamp_probability(mass)


def _source_disagreement(bucket: TemperatureBucket, source_members_by_source: Mapping[str, Sequence[float]]) -> tuple[float, dict[str, float]]:
    source_probs: dict[str, float] = {}
    for source, members in source_members_by_source.items():
        finite = _finite_members(members)
        if finite:
            source_probs[str(source)] = kde_bucket_probability(bucket, finite)
    if len(source_probs) < 2:
        return 0.0, source_probs
    disagreement = statistics.pstdev(source_probs.values()) / 0.5
    return _clamp_probability(disagreement), source_probs


def estimate_bucket_probability(
    *,
    mean_f: float,
    sigma_f: float,
    bucket: TemperatureBucket,
    p_climo: float,
    effective_member_count: int = 1,
    source_members_by_source: Mapping[str, Sequence[float]] | None = None,
    source_set_used: Sequence[str] | None = None,
    config: ProbabilityEngineConfig | None = None,
) -> ProbabilityEstimate:
    cfg = config or ProbabilityEngineConfig()
    p_climo_yes = _clamp_probability(float(p_climo))
    fit = fit_gumbel_from_mean_sigma(mean_f, sigma_f)
    p_gumbel = gumbel_bucket_probability(bucket, fit)
    source_members = {str(k): list(v) for k, v in (source_members_by_source or {}).items()}
    finite_members_by_source = {k: _finite_members(v) for k, v in source_members.items()}
    finite_members_by_source = {k: v for k, v in finite_members_by_source.items() if v}
    all_members = [value for members in finite_members_by_source.values() for value in members]

    source_count = len(finite_members_by_source)
    p_kde: float | None = None
    kde_available = source_count >= cfg.kde_min_sources and len(all_members) >= cfg.kde_min_members
    if kde_available:
        p_kde = kde_bucket_probability(bucket, all_members)
        p_model = ((1.0 - cfg.kde_blend_weight) * p_gumbel) + (cfg.kde_blend_weight * p_kde)
    else:
        p_model = p_gumbel

    n = max(int(effective_member_count), len(all_members), 1)
    alpha = 1.0 if cfg.pseudo_count == 0 else n / (n + cfg.pseudo_count)
    p_final = _clamp_probability((alpha * p_model) + ((1.0 - alpha) * p_climo_yes))
    disagreement, source_probabilities = _source_disagreement(bucket, finite_members_by_source)
    boundary_mass = _model_boundary_mass(
        bucket=bucket,
        fit=fit,
        source_members_by_source=finite_members_by_source,
        p_kde_available=kde_available,
        config=cfg,
    )
    used_sources = list(source_set_used or finite_members_by_source.keys() or ["gumbel"])

    return ProbabilityEstimate(
        p_bucket_yes=p_final,
        p_gumbel_yes=p_gumbel,
        p_kde_yes=p_kde,
        p_model_yes=_clamp_probability(p_model),
        p_climo_yes=p_climo_yes,
        shrinkage_alpha=alpha,
        boundary_mass=boundary_mass,
        disagreement=disagreement,
        effective_member_count=n,
        source_set_used=used_sources,
        gumbel_fit=fit,
        trace={
            "bucket": bucket.to_dict(),
            "config": {
                "pseudo_count": cfg.pseudo_count,
                "boundary_delta_f": cfg.boundary_delta_f,
                "kde_min_sources": cfg.kde_min_sources,
                "kde_min_members": cfg.kde_min_members,
                "kde_blend_weight": cfg.kde_blend_weight,
            },
            "kde_available": kde_available,
            "source_probabilities": source_probabilities,
            "source_member_counts": {source: len(members) for source, members in finite_members_by_source.items()},
        },
    )


def estimate_mapping_probability(
    *,
    mapping: WeatherMarketMapping,
    mean_f: float,
    sigma_f: float,
    p_climo: float,
    effective_member_count: int = 1,
    source_members_by_source: Mapping[str, Sequence[float]] | None = None,
    config: ProbabilityEngineConfig | None = None,
) -> ProbabilityEstimate:
    return estimate_bucket_probability(
        mean_f=mean_f,
        sigma_f=sigma_f,
        bucket=TemperatureBucket.for_mapping(mapping),
        p_climo=p_climo,
        effective_member_count=effective_member_count,
        source_members_by_source=source_members_by_source,
        config=config,
    )
