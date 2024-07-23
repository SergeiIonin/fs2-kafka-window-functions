package io.github.sergeiionin.chunksizewindows

import cats.effect.{Async, Ref, Resource}
import cats.syntax.functor._
import io.github.sergeiionin.WindowRecordsAggregatorService
import io.github.sergeiionin.WindowRecordsAggregatorService.ChunksMap
import fs2.Chunk
import fs2.kafka.CommittableConsumerRecord
import wvlet.log.Logger

// chunkNumberRef holds the number of the chunk to which the record is attributed, after the cleanup,
// all filled chunks will be released and deleted

class SizeWindowKafkaRecordsAggregatorServiceImpl[F[_]: Async, K, V](
  chunkSize:    Int,
  chunksMapRef: Ref[F, ChunksMap[Long, CommittableConsumerRecord[F, K, V]]],
  releaseChunk: Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit],
)(implicit
  logger:       Logger
) extends WindowRecordsAggregatorService[F, Long, CommittableConsumerRecord[F, K, V]](chunksMapRef) {

  override def getStateKey(rec: CommittableConsumerRecord[F, K, V]): F[Long] = {
    // safe
    def getOrIncrementCurrentKey(currentKey: Long, mapping: ChunksMap[Long, CommittableConsumerRecord[F, K, V]]): Long = {
      val size = mapping(currentKey).size
      if (size < chunkSize)
        currentKey
      else
        currentKey + 1
    }

    for {
      chunksMap <- chunksMapRef.get
      key        =
        chunksMap.keySet.toList.sorted match {
          case Nil  => 0L
          case list => getOrIncrementCurrentKey(list.last, chunksMap)
        }
    } yield key
  }

  override def onChunkRelease(chunk: Chunk[CommittableConsumerRecord[F, K, V]]): F[Unit] = releaseChunk(chunk)

  override def releaseChunkCondition(
    chunksMap: ChunksMap[Long, CommittableConsumerRecord[F, K, V]]
  ): Long => Boolean = {
    val latestKey = chunksMap.keySet.toList.sorted.max
    (key: Long) => (key != latestKey) || chunksMap.get(key).map(_.size).fold(false)(_ == chunkSize)
  }

}

object SizeWindowKafkaRecordsAggregatorServiceImpl {

  def make[F[_]: Async, K, V](
    chunkSize:       Int,
    cleanupInterval: Long,
    releaseChunk:    Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit],
  )(implicit
    logger:          Logger
  ): Resource[F, WindowRecordsAggregatorService[F, Long, CommittableConsumerRecord[F, K, V]]] =
    for {
      chunksRef <- Resource.eval(Async[F].ref(Map(0L -> Chunk.empty[CommittableConsumerRecord[F, K, V]])))
      service    = new SizeWindowKafkaRecordsAggregatorServiceImpl(chunkSize, chunksRef, releaseChunk)
      res       <- WindowRecordsAggregatorService.make(cleanupInterval * 5, service)
    } yield res

}
