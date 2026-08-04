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
    for {
      env      <- ZIO.environment[R]
      fetchR    = fetch.provideEnvironment(env)
      scheduleR = schedule.provideEnvironment(env)
      ref       = new AtomicReference[CacheState[E, A]](CacheState.Loading)
      cache     = new BackgroundCacheLive(ref, fetchR)
      _        <- cache.loop(scheduleR).forkScoped
    } yield cache

}

private final class BackgroundCacheLive[E, A](
  ref: AtomicReference[CacheState[E, A]],
  fetchR: IO[E, A]
) extends BackgroundCache[E, A] {

  override def state(): CacheState[E, A] = ref.get()

  override def refresh(implicit trace: Trace): IO[E, Unit] = runFetch

  private[core] def loop(schedule: Schedule[Any, Any, Any])(implicit trace: Trace): URIO[Any, Unit] =
    runFetch.ignore.repeat(schedule).unit

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
          ZIO.suspendSucceed {
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
            ZIO.logWarningCause("Background cache refresh failed", Cause.fail(error)) *> ZIO.fail(error)
          },
        value =>
          Clock.instant.map { now =>
            ref.updateAndGet(current => if (isFresherThan(current, now)) current else CacheState.Healthy(value, now))
          }.unit
      )
    }

}
