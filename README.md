# zio-background-cache

A tiny ZIO-native cache that refreshes itself in the background on a schedule you control.

## Why

Sometimes you need a value that's expensive or slow to fetch (a remote config, a JWKS, a
feature-flag snapshot, ...) available for **synchronous, effect-free reads** on the hot path,
while a background process keeps it warm. `zio-background-cache` is that: one value, refreshed
periodically, read without touching `ZIO` at all.

## Install

```scala
libraryDependencies += "com.guizmaii" %% "zio-background-cache-core" % "<version>"
```

## Usage

```scala
import zio._
import com.guizmaii.zio.background.cache.core._

val fetchConfig: Task[Config] = ??? // however you get the value

val program: ZIO[Scope, Nothing, Unit] =
  for {
    cache <- BackgroundCache.make(fetchConfig, Schedule.fixed(30.seconds))
    _     <- useTheCache(cache)
  } yield ()

def useTheCache(cache: BackgroundCache[Throwable, Config]): UIO[Unit] =
  cache.get() match {
    case CacheValue.Available(config) => ZIO.succeed(useConfig(config))
    case CacheValue.NotYetAvailable    => ZIO.succeed(useDefaultBehaviour()) // still loading
  }
```

- `BackgroundCache.make(fetch, schedule)` starts fetching immediately in the background and
  returns without waiting for it. The background loop runs for the lifetime of the enclosing
  `Scope`.
- `cache.get(): CacheValue[A]` is a plain, synchronous read: no `ZIO`, never blocks, never fails.
  It's `NotYetAvailable` until the first fetch succeeds, and `Available(value)` with the last
  successfully fetched value after that. Call `.value` on it for a plain `Option[A]`.
- `cache.state(): CacheState[E, A]` exposes more detail: `Loading`,
  `Healthy(value, lastSuccessAt)`, or `Degraded(value, lastSuccessAt, lastError)`.
- `cache.refresh: IO[E, Unit]` forces an immediate fetch on top of the schedule, and fails if that
  fetch fails; `get()` keeps serving the last good value regardless.

## Design: what happens when a refresh fails?

A failed background refresh **never removes the cached value**. `get()` keeps serving the last
successfully fetched value, so a consumer never has to handle a missing value just because the
*next* refresh happened to fail.

The failure isn't swallowed, though: it's surfaced as
`CacheState.Degraded(value, lastSuccessAt, lastError)` via `state()`, alongside when the value was
last known good. Consumers that want to alert on staleness or log the error can inspect it;
consumers that only want the value never have to.

The one case with no value to fall back to is before the very first fetch has succeeded: there,
`state()` is `Loading`, and a failed attempt is retried on the next tick without exposing an
error, since there's nothing meaningful to report yet.

Every failed refresh is also logged via ZIO's built-in logging (`ZIO.logWarningCause`), so it
shows up in your logs even if nothing is polling `state()`.

## Metrics (`zio-background-cache-opentelemetry`)

An optional module records refresh metrics against an
[OpenTelemetry](https://opentelemetry.io) `Meter`, via
[zio-opentelemetry](https://github.com/zio/zio-telemetry). It's a separate artifact so the core
module has no OpenTelemetry dependency at all.

```scala
libraryDependencies += "com.guizmaii" %% "zio-background-cache-opentelemetry" % "<version>"
```

```scala
import com.guizmaii.zio.background.cache.core._
import com.guizmaii.zio.background.cache.opentelemetry._
import zio.telemetry.opentelemetry.core.metrics.Meter

val program: ZIO[Scope & Meter, Nothing, Unit] =
  for {
    instrumentedFetch <- BackgroundCacheMetrics.instrument("config", fetchConfig)
    cache             <- BackgroundCache.make(instrumentedFetch, Schedule.fixed(30.seconds))
    _                 <- useTheCache(cache)
  } yield ()
```

`BackgroundCacheMetrics.instrument(name, fetch)` wraps a fetch effect so every attempt (including
the ones the background loop makes) is recorded, all tagged with a `cache="<name>"` attribute so
several caches can share one `Meter`:

| Metric                              | Type      | Notes                                    |
|--------------------------------------|-----------|-------------------------------------------|
| `background_cache.refresh.total`     | Counter   | Every attempt, success or failure          |
| `background_cache.refresh.success`   | Counter   | Successful attempts                        |
| `background_cache.refresh.failure`   | Counter   | Failed attempts                            |
| `background_cache.refresh.duration`  | Histogram | Fetch duration, in milliseconds            |
| `background_cache.health`            | Gauge     | `1` after a success, `0` after a failure   |

This mirrors the metric set from scala-nimbus-jose-jwt's `JwksManager`.

### Example Grafana dashboard

The panels below assume metrics reach Prometheus through an OTel Collector's Prometheus exporter,
which normalizes OTel instrument names to Prometheus convention (dots to underscores, a `_total`
suffix on counters, the unit spelled out on histograms — `ms` becomes `milliseconds`). Adjust the
metric names below if your pipeline exports metrics differently.

```json
{
  "title": "zio-background-cache",
  "schemaVersion": 39,
  "timezone": "browser",
  "templating": {
    "list": [
      {
        "name": "cache",
        "type": "query",
        "datasource": { "type": "prometheus", "uid": "${datasource}" },
        "query": "label_values(background_cache_health, cache)",
        "multi": true,
        "includeAll": true
      }
    ]
  },
  "panels": [
    {
      "id": 1,
      "title": "Refresh rate (success vs failure)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "targets": [
        {
          "expr": "sum(rate(background_cache_refresh_success_total{cache=~\"$cache\"}[5m])) by (cache)",
          "legendFormat": "{{cache}} success"
        },
        {
          "expr": "sum(rate(background_cache_refresh_failure_total{cache=~\"$cache\"}[5m])) by (cache)",
          "legendFormat": "{{cache}} failure"
        }
      ]
    },
    {
      "id": 2,
      "title": "Refresh duration (p50 / p95 / p99)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
      "targets": [
        {
          "expr": "histogram_quantile(0.50, sum(rate(background_cache_refresh_duration_milliseconds_bucket{cache=~\"$cache\"}[5m])) by (le, cache))",
          "legendFormat": "{{cache}} p50"
        },
        {
          "expr": "histogram_quantile(0.95, sum(rate(background_cache_refresh_duration_milliseconds_bucket{cache=~\"$cache\"}[5m])) by (le, cache))",
          "legendFormat": "{{cache}} p95"
        },
        {
          "expr": "histogram_quantile(0.99, sum(rate(background_cache_refresh_duration_milliseconds_bucket{cache=~\"$cache\"}[5m])) by (le, cache))",
          "legendFormat": "{{cache}} p99"
        }
      ]
    },
    {
      "id": 3,
      "title": "Cache health",
      "type": "stat",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "targets": [
        {
          "expr": "background_cache_health{cache=~\"$cache\"}",
          "legendFormat": "{{cache}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "mappings": [
            { "type": "value", "options": { "0": { "text": "Degraded", "color": "red" } } },
            { "type": "value", "options": { "1": { "text": "Healthy", "color": "green" } } }
          ]
        }
      }
    },
    {
      "id": 4,
      "title": "Total refresh attempts",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
      "targets": [
        {
          "expr": "sum(rate(background_cache_refresh_total{cache=~\"$cache\"}[5m])) by (cache)",
          "legendFormat": "{{cache}}"
        }
      ]
    }
  ]
}
```
