package com.guizmaii.zio.background.cache.opentelemetry

import zio._
import zio.telemetry.opentelemetry.testkit.OpenTelemetryTestkit
import zio.telemetry.opentelemetry.testkit.metrics.MetricData
import zio.telemetry.opentelemetry.testkit.metrics.MetricData.PointData
import zio.telemetry.opentelemetry.testkit.metrics.MeterTestkit
import zio.test._

object BackgroundCacheMetricsSpec extends ZIOSpecDefault {

  private def pointFor[T <: PointData](data: List[MetricData[T]], metricName: String, cache: String): Option[T] =
    data.filter(_.name == metricName).flatMap(_.points).find(_.attributes.asMap.get("cache").contains(cache))

  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("BackgroundCacheMetricsSpec")(
      test("tags every recorded point with the cache attribute, and records success/failure independently per cache") {
        for {
          testkit    <- ZIO.service[MeterTestkit]
          meter      <- testkit.getMeter("BackgroundCacheMetricsSpec")
          metrics    <- BackgroundCacheMetrics.make.provideEnvironment(ZEnvironment(meter))
          _          <- metrics.instrument("widgets", ZIO.succeed(1))
          _          <- metrics.instrument("gadgets", ZIO.fail("boom")).either
          counters   <- testkit.collectCounterMetrics
          gauges     <- testkit.collectGaugeMetrics
          histograms <- testkit.collectHistogramMetrics
        } yield assertTrue(
          pointFor(counters, "background_cache.refresh.total", "widgets").exists(_.value == 1L),
          pointFor(counters, "background_cache.refresh.total", "gadgets").exists(_.value == 1L),
          pointFor(counters, "background_cache.refresh.success", "widgets").exists(_.value == 1L),
          pointFor(counters, "background_cache.refresh.success", "gadgets").isEmpty,
          pointFor(counters, "background_cache.refresh.failure", "gadgets").exists(_.value == 1L),
          pointFor(counters, "background_cache.refresh.failure", "widgets").isEmpty,
          pointFor(gauges, "background_cache.health", "widgets").exists(_.value == 1.0),
          pointFor(gauges, "background_cache.health", "gadgets").exists(_.value == 0.0),
          pointFor(histograms, "background_cache.refresh.duration", "widgets").exists(_.count == 1L),
          pointFor(histograms, "background_cache.refresh.duration", "gadgets").exists(_.count == 1L)
        )
      }
    ).provideSomeLayer[TestEnvironment with Scope](OpenTelemetryTestkit.ctxStorageZioFiberRef >>> MeterTestkit.inMemory)

}
