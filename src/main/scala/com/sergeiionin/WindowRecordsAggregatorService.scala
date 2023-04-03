package com.sergeiionin

import cats.effect.implicits.genSpawnOps
import cats.effect.std.Semaphore
import cats.effect.{Async, Ref, Resource}
import cats.implicits.{catsSyntaxFlatMapOps, catsSyntaxFlatten, toFunctorOps, toTraverseOps}
import cats.syntax.flatMap._
import com.sergeiionin.WindowRecordsAggregatorService.ChunksMap
import fs2.Chunk
import wvlet.log.Logger

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

// developer should only provide the way how to attribute record to the chunk (obtain a key of the
// Map[KR, Chunk[R]], where KR is the key type,
// the release condition of chunks from Map[KR, Chunk[R]
// and the release function Chunk[R] => F[Unit]

abstract class WindowRecordsAggregatorService[F[_]: Async, KR, R](
  chunksRef: Ref[F, ChunksMap[KR, R]]
)(implicit
  logger:    Logger
) {
  def getStateKey(rec: R): F[KR]
  def onChunkRelease(chunk: Chunk[R]): F[Unit]

  // we may need access to all chunks to determine which we should release (see TimeWindowKafkaRecordsAggregatorServiceImpl)
  def releaseChunkCondition(chunksMap: ChunksMap[KR, R]): KR => Boolean

  private def splitChunks(chunksMap: ChunksMap[KR, R]): (ChunksMap[KR, R], ChunksMap[KR, R]) =
    if (chunksMap.nonEmpty) {
      val condition          = releaseChunkCondition(chunksMap)
      val mapChunksReleasing = chunksMap.filter { case (kr, _) => condition(kr) }
      val mapChunksLeft      = chunksMap.removedAll(mapChunksReleasing.keySet)

      mapChunksLeft -> mapChunksReleasing
    } else
      Map.empty[KR, Chunk[R]] -> Map.empty[KR, Chunk[R]]

  // getStateKey should give the same result if chunksRef.update failed
  def addToChunk(rec: R): F[Unit] =
    for {
      mutex <- Semaphore.apply(1)
      _     <- mutex.acquire
      key   <- getStateKey(rec)
      _      = logger.info(s"record will be attributed to the key $key")
      _     <- chunksRef.update(chunksMap => {
                 val currentChunk = chunksMap.getOrElse(key, Chunk.empty[R])
                 chunksMap.updated(key, currentChunk ++ Chunk(rec))
               })
      _     <- mutex.release
    } yield ()

  def clear(durationMillis: Long): F[Unit] =
    fs2.Stream
      .awakeEvery(FiniteDuration(durationMillis, MILLISECONDS))
      .evalMap(_ =>
        chunksRef.modify { chunksMap =>
          {
            val (chunksLeft, chunksReleased) = splitChunks(chunksMap)
            chunksLeft -> chunksReleased.values.toList.traverse(onChunkRelease)
          }
        }.flatten
      )
      .compile
      .drain

  def finalizeChunks(): F[Unit] =
    chunksRef.modify { chunksMap =>
      {
        Map.empty[KR, Chunk[R]] -> {
          val chunksLeft = chunksMap.values.toList
          logger.info(s"exiting clearingStream, number of chunks left to release is ${chunksLeft.size}...")
          chunksMap.values.toList.traverse(onChunkRelease).void
        }
      }
    }.flatten

}

object WindowRecordsAggregatorService {

  type ChunksMap[KR, R] = Map[KR, Chunk[R]]

  def make[F[_]: Async, KR, R](
    durationMillis: Long,
    service:        WindowRecordsAggregatorService[F, KR, R],
  ): Resource[F, WindowRecordsAggregatorService[F, KR, R]] = {

    def backgroundResource() = Resource
      .make(service.clear(durationMillis).start)(fib => service.finalizeChunks() >> fib.cancel)
      .map(_.join)

    for {
      _    <- backgroundResource()
      main <- Resource.pure(service)
    } yield main
  }
}
