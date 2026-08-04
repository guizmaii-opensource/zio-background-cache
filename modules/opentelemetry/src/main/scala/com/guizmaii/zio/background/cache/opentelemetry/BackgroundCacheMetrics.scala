package com.guizmaii.zio.background.cache.opentelemetry

import io.opentelemetry.api.common.{AttributeKey, Attributes}
import zio._
import zio.telemetry.opentelemetry.core.metrics.Meter

/**
 * Records refresh metrics for a `BackgroundCache` against an OpenTelemetry `Meter`, mirroring the
 * metric set used by scala-nimbus-jose-jwt's JwksManager: total/success/failure counters, a
 * refresh duration histogram, and a health gauge.
 */
object BackgroundCacheMetrics {

  private val CacheNameKey = AttributeKey.stringKey("cache")

  /**
   * Wraps `fetch` so every refresh attempt is recorded against the `Meter` in the environment.
   * Build once per cache and pass the returned effect to `BackgroundCache.make`.
   *
   * Records, all tagged with a `cache` attribute set to `name`:
   *   - `background_cache.refresh.total` / `.success` / `.failure` counters
   *   - `background_cache.refresh.duration` histogram, in milliseconds
   *   - `background_cache.health` gauge: `1` after a successful attempt, `0` after a failed one
   */
  def instrument[R, E, A](name: String, fetch: ZIO[R, E, A])(implicit trace: Trace): URIO[Meter, ZIO[R, E, A]] =
    ZIO.serviceWith[Meter] { meter =>
      val total      = meter.counter("background_cache.refresh.total")
      val success    = meter.counter("background_cache.refresh.success")
      val failure    = meter.counter("background_cache.refresh.failure")
      val duration   = meter.histogram("background_cache.refresh.duration", unit = Some("ms"))
      val health     = meter.gauge("background_cache.health")
      val attributes = Attributes.of(CacheNameKey, name)

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
