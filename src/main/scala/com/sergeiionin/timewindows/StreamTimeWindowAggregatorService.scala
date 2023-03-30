package com.sergeiionin.timewindows

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref}
import cats.implicits.{catsSyntaxApplicativeId, toFunctorOps, toTraverseOps}
import cats.syntax.flatMap._
import fs2.Chunk
import wvlet.log.Logger

abstract class StreamTimeWindowAggregatorService[F[_]: Async, R](timeWindowMillis: Long)(implicit logger: Logger) {

  val chunkState: Ref[F, Map[Long, Chunk[R]]]
  def getStateKey(rec: R): F[Long]
  def onChunkRelease(chunk: Chunk[R]): F[Unit]
  def onRelease(): F[Unit] =
    chunkState.modify { chunksMap =>
      {
        Map.empty[Long, Chunk[R]] -> chunksMap.values.toList.traverse(onChunkRelease).void
      }
    }.flatten

  // release chunks which were added more than timeWindowMillis * 5 millis ago
  private def releaseChunks(chunksMap: Map[Long, Chunk[R]]): (Map[Long, Chunk[R]], F[Unit]) = {
    if (chunksMap.isEmpty)
      chunksMap -> ().pure[F]
    else {
      val keysSortedAsc = chunksMap.keySet.toList.sorted
      val keyMax        = keysSortedAsc.last
      val keysReleasing = keysSortedAsc.filter(_ <= (keyMax - 2 * timeWindowMillis))
      val keysLeft      = keysSortedAsc diff keysReleasing

      val mapChunksReleasing = chunksMap.removedAll(keysLeft)
      val mapChunksLeft      = chunksMap.removedAll(keysReleasing)

      logger.info(s"${System.currentTimeMillis()} releasing old chunks")
      mapChunksLeft -> mapChunksReleasing.values.toList.traverse(onChunkRelease).void
    }
  }

  private val mutexF = Semaphore.apply(1)

  // todo we can release chunks here if they are e.g. very old
  def addToChunk(rec: R): F[Unit] =
    for {
      mutex <- mutexF
      _     <- mutex.acquire
      key   <- getStateKey(rec)
      _      = logger.info(s"record will be attributed to the key $key")
      _     <-
        chunkState
          .modify(chunksMap => {
            val currentChunk = chunksMap.getOrElse(key, Chunk.empty[R])
            val mapUpd       = chunksMap.updated(key, currentChunk ++ Chunk(rec))
            releaseChunks(mapUpd)
          })
          .flatten
      _     <- mutex.release
    } yield ()

}
