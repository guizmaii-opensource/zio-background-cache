package com.guizmaii.zio.background.cache.core

/**
 * The value side of a [[BackgroundCache]]'s [[CacheState]], without the refresh-health detail.
 *
 * Returned by [[BackgroundCache.get]] for consumers who only care whether a value is available
 * yet, not why it might not be (or whether the background loop is currently failing). Call
 * [[CacheValue.value]] to fall back to a plain `Option[A]`.
 */
enum CacheValue[+A] {
  case NotYetAvailable extends CacheValue[Nothing]
  case Available(a: A) extends CacheValue[A]

  def value: Option[A] =
    this match {
      case CacheValue.NotYetAvailable => None
      case CacheValue.Available(a)    => Some(a)
    }
}
