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
