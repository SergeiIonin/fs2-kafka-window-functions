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

abstract class WindowRecordsAggregatorService[F[_]: Async, R](chunksRef: Ref[F, ChunksMap[R]]
                                                             )(implicit logger: Logger) {

  //val chunkState: Ref[F, Map[Long, Chunk[R]]]
  //def getStateKey(rec: R): F[Long]

  def getStateKey(rec: R): F[Long]
  def splitChunks(chunksMap: ChunksMap[R]): (ChunksMap[R], ChunksMap[R])
  def onChunkRelease(chunk: Chunk[R]): F[Unit]

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

  def clear(durationMillis: Long): F[Unit] = {
    fs2.Stream
      .awakeEvery(FiniteDuration(durationMillis, MILLISECONDS))
      .evalMap(_ =>
        chunksRef.modify { chunksMap => {
          val (chunksLeft, chunksReleased) = splitChunks(chunksMap)
          chunksLeft -> chunksReleased.values.toList.traverse(onChunkRelease)
        }
        }.flatten
      )
      .compile
      .drain
  }

  def finalizeChunks(): F[Unit] = chunksRef.modify { chunksMap => {
    Map.empty[Long, Chunk[R]] -> {
      val chunksLeft = chunksMap.values.toList
      logger.info(s"exiting clearingStream, number of chunks left to release is ${chunksLeft.size}...")
      chunksMap.values.toList.traverse(onChunkRelease).void
    }
  }
  }.flatten


}

object WindowRecordsAggregatorService {

  type ChunksMap[R] = Map[Long, Chunk[R]]

  def make[F[_] : Async, R](durationMillis: Long,
                            service: WindowRecordsAggregatorService[F, R]): Resource[F, WindowRecordsAggregatorService[F, R]] = {

  def backgroundResource() =
      Resource
        .make(service.clear(durationMillis).start)(fib =>
          service.finalizeChunks() >> fib.cancel
        ).map(_.join)

      for {
        _         <- backgroundResource()
        main      <- Resource.pure(service)
      } yield main
  }
}

