package com.guizmaii.zio.background.cache.opentelemetry

import zio._
import zio.telemetry.opentelemetry.testkit.OpenTelemetryTestkit
import zio.telemetry.opentelemetry.testkit.metrics.{MeterTestkit, MetricData}
import zio.test._

object BackgroundCacheMetricsSpec extends ZIOSpecDefault {

  private def counterValue(counters: List[MetricData.Counter], name: String): Long =
    counters.filter(_.name == name).flatMap(_.points).map(_.value).sum

  private def histogramCount(histograms: List[MetricData.Histogram], name: String): Long =
    histograms.filter(_.name == name).flatMap(_.points).map(_.count).sum

  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("BackgroundCacheMetricsSpec")(
      test("records total/success/failure counters, a duration histogram, and a health gauge per attempt") {
        for {
          testkit         <- ZIO.service[MeterTestkit]
          meter           <- testkit.getMeter("BackgroundCacheMetricsSpec")
          instrumentedOk  <- BackgroundCacheMetrics.instrument("widgets", ZIO.succeed(1)).provideEnvironment(ZEnvironment(meter))
          instrumentedBad <- BackgroundCacheMetrics.instrument("widgets", ZIO.fail("boom")).provideEnvironment(ZEnvironment(meter))
          _               <- instrumentedOk
          _               <- instrumentedBad.either
          counters        <- testkit.collectCounterMetrics
          gauges          <- testkit.collectGaugeMetrics
          histograms      <- testkit.collectHistogramMetrics
        } yield assertTrue(
          counterValue(counters, "background_cache.refresh.total") == 2L,
          counterValue(counters, "background_cache.refresh.success") == 1L,
          counterValue(counters, "background_cache.refresh.failure") == 1L,
          histogramCount(histograms, "background_cache.refresh.duration") == 2L,
          gauges.exists(_.name == "background_cache.health")
        )
      }
    ).provideSomeLayer[TestEnvironment with Scope](OpenTelemetryTestkit.ctxStorageZioFiberRef >>> MeterTestkit.inMemory)

}
