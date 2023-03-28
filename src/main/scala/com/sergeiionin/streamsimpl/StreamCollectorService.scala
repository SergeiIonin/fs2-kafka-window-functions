package com.sergeiionin.streamsimpl

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref}
import cats.implicits.{catsSyntaxApplicativeId, toFunctorOps}
import cats.syntax.flatMap._
import fs2.Chunk
import wvlet.log.Logger

abstract class StreamCollectorService[F[_] : Async, R](implicit logger: Logger) {
  val state: Ref[F, Chunk[R]]
  val addCond: R => F[Boolean]
  val releaseCond: Chunk[R] => F[Boolean]
  val onRelease: Chunk[R] => F[Unit]

  private val mutexF = Semaphore.apply(1)

  def addToChunk(rec: R): F[Unit] =
    for {
      mutex <- mutexF
      _     <- mutex.acquire
      cond  <- addCond(rec)
      _ = logger.info(s"cond for $rec is $cond")
      chunk <- state.get
      chunkUpd = if (cond) chunk ++ Chunk(rec) else chunk
      _ = logger.info(s"chunkUpd = $chunkUpd")
      shouldRelease <- releaseCond(chunkUpd)
      _ = logger.info(s"should release $chunkUpd is $shouldRelease")
      update = if (shouldRelease) {
                  Chunk.empty[R] -> onRelease(chunkUpd)
               } else {
                  chunkUpd -> ().pure[F]
               }
      _ <- state.modify(_ => update).flatten
      _ <- mutex.release
    } yield ()

}
