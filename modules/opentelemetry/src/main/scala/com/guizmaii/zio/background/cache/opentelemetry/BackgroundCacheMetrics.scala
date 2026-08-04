package com.guizmaii.zio.background.cache.opentelemetry

import io.opentelemetry.api.common.{AttributeKey, Attributes}
import zio._
import zio.telemetry.opentelemetry.core.metrics.{Counter, Gauge, Histogram, Meter}

/**
 * Records refresh metrics for `BackgroundCache`s against an OpenTelemetry `Meter`, mirroring the
 * metric set used by scala-nimbus-jose-jwt's JwksManager: total/success/failure counters, a
 * refresh duration histogram, and a health gauge.
 *
 * Build once per `Meter` via [[BackgroundCacheMetrics.make]], then call [[instrument]] once per
 * cache: the underlying instruments are shared, only the `cache` attribute varies per call.
 */
final class BackgroundCacheMetrics private (
  total: Counter[Long],
  success: Counter[Long],
  failure: Counter[Long],
  duration: Histogram[Double],
  health: Gauge[Double]
) {

  /**
   * Wraps `fetch` so every refresh attempt is recorded, all tagged with a `cache` attribute set to
   * `name`. Pass the returned effect to `BackgroundCache.make`.
   *
   *   - `background_cache.refresh.total` / `.success` / `.failure` counters
   *   - `background_cache.refresh.duration` histogram, in milliseconds
   *   - `background_cache.health` gauge: `1` after a successful attempt, `0` after a failed one
   */
  def instrument[R, E, A](name: String, fetch: ZIO[R, E, A])(implicit trace: Trace): ZIO[R, E, A] = {
    val attributes = Attributes.of(BackgroundCacheMetrics.CacheNameKey, name)

    total.add(1L, attributes) *>
      fetch.either.timed.flatMap { case (elapsed, outcome) =>
        val outcomeCounter = if (outcome.isRight) success else failure
        outcomeCounter.add(1L, attributes) *>
          duration.record(elapsed.toMillis.toDouble, attributes) *>
          health.set(if (outcome.isRight) 1.0 else 0.0, attributes) *>
          ZIO.fromEither(outcome)
      }
  }

}

object BackgroundCacheMetrics {

  private val CacheNameKey = AttributeKey.stringKey("cache")

  def make(implicit trace: Trace): URIO[Meter, BackgroundCacheMetrics] =
    ZIO.serviceWith[Meter] { meter =>
      new BackgroundCacheMetrics(
        total = meter.counter("background_cache.refresh.total"),
        success = meter.counter("background_cache.refresh.success"),
        failure = meter.counter("background_cache.refresh.failure"),
        duration = meter.histogram("background_cache.refresh.duration", unit = Some("ms")),
        health = meter.gauge("background_cache.health")
      )
    }

}
