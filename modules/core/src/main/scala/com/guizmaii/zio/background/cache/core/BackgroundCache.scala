package com.guizmaii.zio.background.cache.core

import zio._

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

/**
 * A single value, kept warm by a background refresh loop.
 *
 * `get()` is a plain, synchronous read of the current state: it never blocks, never fails, and
 * involves no `ZIO` effect. A failing refresh never makes a previously fetched value unavailable;
 * it is only reflected in [[state]].
 */
trait BackgroundCache[+E, +A] {

  /** The current state: whether a value is available yet, and whether the background refresh loop is healthy. */
  def state(): CacheState[E, A]

  /** The last successfully fetched value, if any. Always available, never blocks, never fails. */
  final def get(): CacheValue[A] =
    state() match {
      case CacheState.Loading               => CacheValue.NotYetAvailable
      case CacheState.Healthy(value, _)     => CacheValue.Available(value)
      case CacheState.Degraded(value, _, _) => CacheValue.Available(value)
    }

  /**
   * Forces an immediate refresh, on top of the periodic schedule. Fails if the fetch fails, but
   * [[get]] keeps serving the last good value regardless, and [[state]] is updated either way.
   */
  def refresh(implicit trace: Trace): IO[E, Unit]

}

object BackgroundCache {

  /**
   * Builds a cache whose value is fetched and kept refreshed in the background according to
   * `schedule`, including the very first fetch: `make` returns immediately, before any value is
   * available. [[BackgroundCache.get]] returns `CacheValue.NotYetAvailable` until the first fetch
   * succeeds.
   *
   * The background refresh loop runs for the lifetime of the enclosing `Scope`. Its failures
   * never terminate it: they are recorded in [[BackgroundCache.state]] and the loop tries again
   * on the next tick.
   */
  def make[R, E, A](
    fetch: ZIO[R, E, A],
    schedule: Schedule[R, Any, Any]
  )(implicit trace: Trace): URIO[R & Scope, BackgroundCache[E, A]] =
    make(fetch, schedule, schedule)

  /**
   * Like [[make]], but retries on `warmupSchedule` instead of `schedule` while the cache is still
   * `Loading` (no fetch has ever succeeded) - useful to retry more aggressively while cold-starting
   * than during steady-state operation. Once the first fetch succeeds, the cache switches to
   * `schedule` for good, even if a later fetch fails: `warmupSchedule` only ever applies before
   * the first success.
   *
   * Note this still never blocks or fails `make` itself: a consumer that needs to know, at
   * construction time, whether the first fetch actually succeeded wants [[makeAwaitingFirst]]
   * instead.
   */
  def make[R, E, A](
    fetch: ZIO[R, E, A],
    schedule: Schedule[R, Any, Any],
    warmupSchedule: Schedule[R, Any, Any]
  )(implicit trace: Trace): URIO[R & Scope, BackgroundCache[E, A]] =
    for {
      env            <- ZIO.environment[R]
      fetchR          = fetch.provideEnvironment(env)
      scheduleR       = schedule.provideEnvironment(env)
      warmupScheduleR = warmupSchedule.provideEnvironment(env)
      ref             = new AtomicReference[CacheState[E, A]](CacheState.Loading)
      cache           = new BackgroundCacheLive(ref, fetchR)
      _              <- cache.loop(scheduleR, warmupScheduleR).forkScoped
    } yield cache

  /**
   * Builds a cache like [[make]], but performs the first fetch synchronously before returning,
   * retrying it on `bootRetrySchedule` (a single attempt, no retries, by default). If every
   * attempt fails, `makeAwaitingFirst` itself fails with the same error - no cache is returned,
   * and no background loop is started.
   *
   * Unlike [[make]]'s `warmupSchedule`, `bootRetrySchedule` sees the real `E` failure on every
   * attempt: there is no cache state yet for it to reason about, so it retries the bare `fetch`
   * effect directly, same as calling `fetch.retry(bootRetrySchedule)` yourself.
   *
   * A cache built this way is never observed as `Loading`: by the time `makeAwaitingFirst`
   * returns, a fetch has already succeeded, and the background refresh loop - governed by
   * `schedule` alone, with no warmup phase of its own - only starts after that.
   */
  def makeAwaitingFirst[R, E, A](
    fetch: ZIO[R, E, A],
    schedule: Schedule[R, Any, Any],
    bootRetrySchedule: Schedule[R, E, Any] = Schedule.stop
  )(implicit trace: Trace): ZIO[R & Scope, E, BackgroundCache[E, A]] =
    for {
      env      <- ZIO.environment[R]
      fetchR    = fetch.provideEnvironment(env)
      scheduleR = schedule.provideEnvironment(env)
      bootR     = bootRetrySchedule.provideEnvironment(env)
      ref       = new AtomicReference[CacheState[E, A]](CacheState.Loading)
      cache     = new BackgroundCacheLive(ref, fetchR)
      _        <- cache.refresh.retry(bootR)
      _        <- cache.steadyStateLoop(scheduleR).forkScoped
    } yield cache

}

private final class BackgroundCacheLive[E, A](
  ref: AtomicReference[CacheState[E, A]],
  fetchR: IO[E, A]
) extends BackgroundCache[E, A] {

  override def state(): CacheState[E, A] = ref.get()

  override def refresh(implicit trace: Trace): IO[E, Unit] = runFetch

  // `foldZIO` in `runFetch` below only observes typed `E` failures: a defect (an unexpected
  // exception `fetch` didn't wrap into `E`) would otherwise propagate straight through `.ignore`
  // and kill this forked fiber, silently ending all future refreshes. `catchAllDefect` only
  // intercepts defects (never interruption, which must keep propagating so `forkScoped` cleanup
  // still works), and there's no `E` value to record it against, so it's just logged and the
  // schedule retries.
  private def attempt(implicit trace: Trace): URIO[Any, Unit] =
    runFetch
      .catchAllDefect(defect => ZIO.logWarningCause("Background cache refresh attempt died unexpectedly", Cause.die(defect)))
      .ignore

  // Runs in two phases: `warmupSchedule` drives retries while still `Loading` (stopping the
  // moment a fetch succeeds, via `whileInput`, not just when `warmupSchedule` itself is
  // exhausted), then `steadyStateLoop` takes over for good.
  private[core] def loop(schedule: Schedule[Any, Any, Any], warmupSchedule: Schedule[Any, Any, Any])(
    implicit trace: Trace
  ): URIO[Any, Unit] = {
    def isLoading: Boolean =
      ref.get() match {
        case CacheState.Loading => true
        case _                  => false
      }

    attempt.repeat(warmupSchedule.whileInput((_: Any) => isLoading)) *> steadyStateLoop(schedule)
  }

  // Driven manually (rather than via `.repeat`, which always runs its effect once immediately)
  // so that a caller which already knows the cache is past its first fetch - either `loop` above,
  // handing off after warmup, or `makeAwaitingFirst`, whose synchronous boot fetch already
  // succeeded - never pays for a redundant, immediate extra fetch on top of that.
  private[core] def steadyStateLoop(schedule: Schedule[Any, Any, Any])(implicit trace: Trace): URIO[Any, Unit] =
    for {
      driver <- schedule.driver
      _      <- (driver.next(()) *> attempt).forever.ignore
    } yield ()

  // A manual `refresh` can race a scheduled tick. Before every write, checks whether what's
  // already there is fresher than what triggered this write; if so, keeps it. This way the
  // slower of two concurrent fetches can never resurrect stale data or wrongly downgrade a value
  // a faster one already installed.
  private def isFresherThan(current: CacheState[E, A], at: Instant): Boolean =
    current match {
      case CacheState.Loading           => false
      case CacheState.Healthy(_, t)     => t.isAfter(at)
      case CacheState.Degraded(_, t, _) => t.isAfter(at)
    }

  private def runFetch(implicit trace: Trace): IO[E, Unit] =
    Clock.instant.flatMap { attemptStartedAt =>
      fetchR.foldZIO(
        error =>
          Clock.instant.flatMap { attemptFailedAt =>
            ref.updateAndGet { current =>
              if (isFresherThan(current, attemptStartedAt)) current
              else
                current match {
                  // No value to fall back to yet: stay Loading, the schedule will retry.
                  case CacheState.Loading                           => CacheState.Loading
                  case CacheState.Healthy(value, lastSuccessAt)     => CacheState.Degraded(value, lastSuccessAt, error)
                  case CacheState.Degraded(value, lastSuccessAt, _) => CacheState.Degraded(value, lastSuccessAt, error)
                }
            }
            ZIO.logDebug(
              s"Background cache refresh attempt failed in ${millisBetween(attemptStartedAt, attemptFailedAt)}ms"
            ) *>
              ZIO.logWarningCause("Background cache refresh failed", Cause.fail(error)) *>
              ZIO.fail(error)
          },
        value =>
          Clock.instant.flatMap { now =>
            ref.updateAndGet(current => if (isFresherThan(current, now)) current else CacheState.Healthy(value, now))
            ZIO.logDebug(s"Background cache refresh attempt succeeded in ${millisBetween(attemptStartedAt, now)}ms")
          }
      )
    }

  private def millisBetween(start: Instant, end: Instant): Long = end.toEpochMilli - start.toEpochMilli

}
