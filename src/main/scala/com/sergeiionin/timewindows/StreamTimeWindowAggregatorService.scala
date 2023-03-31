package com.sergeiionin.timewindows

import cats.effect.implicits.genSpawnOps
import cats.effect.std.Semaphore
import cats.effect.{Async, Ref, Resource}
import cats.implicits.{toFunctorOps, toTraverseOps}
import cats.syntax.flatMap._
import fs2.Chunk
import wvlet.log.Logger

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

abstract class StreamTimeWindowAggregatorService[F[_]: Async, R](implicit logger: Logger) {

  val chunkState: Ref[F, Map[Long, Chunk[R]]]
  def getStateKey(rec: R): F[Long]

  private val mutexF = Semaphore.apply(1)

  def addToChunk(rec: R): F[Unit] =
    for {
      mutex <- mutexF
      _     <- mutex.acquire
      key   <- getStateKey(rec)
      _      = logger.info(s"record will be attributed to the key $key")
      _     <- chunkState.update(chunksMap => {
                 val currentChunk = chunksMap.getOrElse(key, Chunk.empty[R])
                 chunksMap.updated(key, currentChunk ++ Chunk(rec))
               })
      _     <- mutex.release
    } yield ()

}

object StreamTimeWindowAggregatorService {

  def clearingStreamResource[F[_]: Async, R](
    durationMillis: Long,
    chunksRef:      Ref[F, Map[Long, Chunk[R]]],
    onRelease:      Chunk[R] => F[Unit],
  )(implicit
    logger:         Logger
  ) = Resource
    .make(
      fs2.Stream
        .awakeEvery(FiniteDuration(durationMillis * 10, MILLISECONDS))
        .evalMap(_ =>
          chunksRef.modify { chunksMap =>
            {
              val keysSortedAsc      = chunksMap.keySet.toList.sorted
              val keyLeft            = keysSortedAsc.lastOption
              val mapChunksReleasing = keyLeft.fold(Map.empty[Long, Chunk[R]])(key => chunksMap.removed(key))
              val chunksMapUpd       = keyLeft.fold(Map.empty[Long, Chunk[R]])(key => Map(key -> chunksMap(key)))

              chunksMapUpd -> {
                logger.info(s"${System.currentTimeMillis()} in the clearing stream")
                mapChunksReleasing.values.toList.traverse(onRelease)
              }
            }
          }.flatten
        )
        .compile
        .drain
        .start
    )(fib => {
      chunksRef.modify { chunksMap =>
        {
          Map.empty[Long, Chunk[R]] -> {
            val chunksLeft = chunksMap.values.toList
            logger.info(s"exiting clearingStream, number of chunks left to release is ${chunksLeft.size}...")
            chunksMap.values.toList.traverse(onRelease).void
          }
        }
      }.flatten >> fib.cancel
    })
    .map(_.join)

}
