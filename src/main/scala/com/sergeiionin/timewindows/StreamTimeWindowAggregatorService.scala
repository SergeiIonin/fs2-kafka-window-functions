package com.sergeiionin.timewindows

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref}
import cats.implicits.toFunctorOps
import cats.syntax.flatMap._
import fs2.Chunk
import wvlet.log.Logger

abstract class StreamTimeWindowAggregatorService[F[_] : Async, R](implicit logger: Logger) {

  val chunkState: Ref[F, Map[Long, Chunk[R]]]
  def getStateKey(rec: R): F[Long]

  private val mutexF = Semaphore.apply(1)

  def addToChunk(rec: R): F[Unit] =
    for {
      mutex         <- mutexF
      _             <- mutex.acquire
      key           <- getStateKey(rec)
      _             = logger.info(s"record will be attributed to the key $key")
      _             <- chunkState.update(chunksMap => {
                          val currentChunk = chunksMap.getOrElse(key, Chunk.empty[R])
                          chunksMap.updated(key, currentChunk ++ Chunk(rec))
                       })
      _             <- mutex.release
    } yield ()

}
