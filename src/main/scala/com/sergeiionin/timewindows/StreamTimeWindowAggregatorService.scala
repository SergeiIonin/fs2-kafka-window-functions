package com.sergeiionin.timewindows

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref}
import cats.implicits.{catsSyntaxApplicativeId, toFunctorOps}
import cats.syntax.flatMap._
import fs2.Chunk
import wvlet.log.Logger

abstract class StreamTimeWindowAggregatorService[F[_] : Async, R](implicit logger: Logger) {

  val chunkState: Ref[F, Chunk[R]]
  def addCond(rec: R): F[Boolean]
  def releaseCond(rec: R): F[Boolean]
  def onRelease(chunk: Chunk[R]): F[Unit]

  private val mutexF = Semaphore.apply(1)

  def addToChunk(rec: R): F[Unit] =
    for {
      mutex         <- mutexF
      _             <- mutex.acquire
      cond          <- addCond(rec)
      _             = logger.info(s"cond for $rec is $cond")
      chunk         <- chunkState.get
      chunkUpd      = if (cond) chunk ++ Chunk(rec) else chunk
      _             = logger.info(s"chunkUpd = $chunkUpd")
      shouldRelease <- releaseCond(rec) // if the rec is obsolete, then it's just discarded (or may be sent to some queue)
      _             = logger.info(s"should release $chunkUpd is $shouldRelease")
      update        = if (shouldRelease) {
                        Chunk.singleton(rec) -> onRelease(chunkUpd)
                      } else {
                        chunkUpd -> ().pure[F]
                      }
      _             <- chunkState.modify(_ => update).flatten
      _             <- mutex.release
    } yield ()

}
