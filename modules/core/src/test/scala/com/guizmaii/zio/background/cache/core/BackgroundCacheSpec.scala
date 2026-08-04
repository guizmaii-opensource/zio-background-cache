package com.guizmaii.zio.background.cache.core

import zio._
import zio.test._

import java.time.Instant

object BackgroundCacheSpec extends ZIOSpecDefault {

  /**
   * A fetch effect driven by a fixed script: call `n` returns `script(n)`, and any call past the end of the script
   * keeps returning the last entry.
   */
  private def scriptedFetch(script: Vector[Either[String, Int]], calls: Ref[Int]): IO[String, Int] =
    calls.getAndUpdate(_ + 1).flatMap(n => ZIO.fromEither(script(n.min(script.length - 1))))

  /**
   * Polls `cache.state()` until it matches `p`. The background loop mutates state via a plain, synchronous
   * `AtomicReference`, so this converges essentially immediately; it just bridges the gap between a `TestClock`
   * tick and the background fiber having actually run. Bounded by a real (not `TestClock`) timeout, so a
   * regression that never satisfies `p` fails the test instead of hanging the suite.
   */
  private def awaitState[E, A](cache: BackgroundCache[E, A])(p: CacheState[E, A] => Boolean): UIO[CacheState[E, A]] =
    Live.live(ZIO.succeed(cache.state()).repeatUntil(p).timeoutFail(new RuntimeException("state never matched"))(5.seconds)).orDie

  private def lastSuccessAt(state: CacheState[?, ?]): Option[Instant] =
    state match {
      case CacheState.Loading           => None
      case CacheState.Healthy(_, t)     => Some(t)
      case CacheState.Degraded(_, t, _) => Some(t)
    }

  def spec: Spec[TestEnvironment with Scope, Any] = suite("BackgroundCacheSpec")(
    suite("BackgroundCache.make")(
      test("starts in Loading before the first fetch completes, then becomes Healthy") {
        for {
          gate   <- Promise.make[Nothing, Unit]
          fetch   = gate.await.as(1)
          result <- ZIO.scoped {
                      for {
                        cache     <- BackgroundCache.make(fetch, Schedule.spaced(1.day))
                        loading    = cache.state()
                        loadingGet = cache.get()
                        _         <- gate.succeed(())
                        healthy   <- awaitState(cache) {
                                       case CacheState.Healthy(_, _) => true
                                       case _                        => false
                                     }
                      } yield assertTrue(
                        loading == CacheState.Loading,
                        loadingGet == CacheValue.NotYetAvailable,
                        loadingGet.value == None,
                        healthy match {
                          case CacheState.Healthy(1, _) => true
                          case _                        => false
                        }
                      )
                    }
        } yield result
      }
    ),
    suite("BackgroundCache::state")(
      test("keeps the last good value and moves to Degraded when a refresh fails, then recovers on the next tick") {
        for {
          calls  <- Ref.make(0)
          fetch   = scriptedFetch(Vector(Right(1), Left("boom"), Right(2)), calls)
          result <- ZIO.scoped {
                      for {
                        cache     <- BackgroundCache.make(fetch, Schedule.spaced(1.second))
                        healthy   <- awaitState(cache) {
                                       case CacheState.Healthy(1, _) => true
                                       case _                        => false
                                     }
                        _         <- TestClock.adjust(1.second)
                        degraded  <- awaitState(cache) {
                                       case CacheState.Degraded(1, _, "boom") => true
                                       case _                                 => false
                                     }
                        _         <- TestClock.adjust(1.second)
                        recovered <- awaitState(cache) {
                                       case CacheState.Healthy(2, _) => true
                                       case _                        => false
                                     }
                      } yield assertTrue(
                        lastSuccessAt(degraded) == lastSuccessAt(healthy),
                        lastSuccessAt(recovered) != lastSuccessAt(healthy)
                      )
                    }
        } yield result
      }
    ),
    suite("BackgroundCache::refresh")(
      test("propagates the fetch error to the caller while still updating state") {
        for {
          calls  <- Ref.make(0)
          fetch   = scriptedFetch(Vector(Right(1), Left("boom")), calls)
          result <- ZIO.scoped {
                      for {
                        cache   <- BackgroundCache.make(fetch, Schedule.spaced(1.day))
                        _       <- awaitState(cache) {
                                     case CacheState.Healthy(1, _) => true
                                     case _                        => false
                                   }
                        outcome <- cache.refresh.either
                      } yield assertTrue(
                        outcome == Left("boom"),
                        cache.get() == CacheValue.Available(1),
                        cache.get().value == Some(1),
                        cache.state() match {
                          case CacheState.Degraded(1, _, "boom") => true
                          case _                                 => false
                        }
                      )
                    }
        } yield result
      }
    )
  )

}
