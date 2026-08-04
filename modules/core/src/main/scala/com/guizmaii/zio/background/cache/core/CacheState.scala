package com.guizmaii.zio.background.cache.core

import java.time.Instant

/**
 * The state of a [[BackgroundCache]] at a point in time.
 *
 * `Healthy` and `Degraded` both carry the last successfully fetched value: a failing refresh
 * never removes it, it only moves the cache into `Degraded` alongside the error that caused it.
 * `Loading` is the state before the very first fetch has succeeded.
 */
enum CacheState[+E, +A] {
  case Loading                                                  extends CacheState[Nothing, Nothing]
  case Healthy(value: A, lastSuccessAt: Instant)                extends CacheState[Nothing, A]
  case Degraded(value: A, lastSuccessAt: Instant, lastError: E) extends CacheState[E, A]
}
