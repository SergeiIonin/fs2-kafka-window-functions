package com.sergeiionin.timewindows

import cats.effect.{Async, Ref, Resource}
import cats.implicits.catsSyntaxFlatMapOps
import cats.syntax.functor._
import com.sergeiionin.WindowRecordsAggregatorService
import com.sergeiionin.WindowRecordsAggregatorService.ChunksMap
import fs2.Chunk
import fs2.kafka.CommittableConsumerRecord
import wvlet.log.Logger

class TimeWindowKafkaRecordsAggregatorServiceImpl[F[_]: Async, K, V](
  timeWindowMillis: Long,
  startRef:         Ref[F, Long],
  chunksRef:        Ref[F, ChunksMap[Long, CommittableConsumerRecord[F, K, V]]],
  releaseChunk:     Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit],
)(implicit
  logger:           Logger
) extends WindowRecordsAggregatorService[F, Long, CommittableConsumerRecord[F, K, V]](chunksRef) {

  override def addToChunk(rec: CommittableConsumerRecord[F, K, V]): F[Unit] =
    startRef.update(time => {
      if (time == 0)
        rec.record.timestamp.createTime.getOrElse(System.currentTimeMillis())
      else
        time
    }) >> super.addToChunk(rec)

  override def getStateKey(rec: CommittableConsumerRecord[F, K, V]): F[Long] =
    for {
      start       <- startRef.get
      recTimestamp = rec.record.timestamp.createTime.getOrElse(System.currentTimeMillis())
      diff         = recTimestamp - start
      key          = start + (diff / timeWindowMillis) * timeWindowMillis
    } yield key

  override def releaseChunkCondition(
    chunksMap: ChunksMap[Long, CommittableConsumerRecord[F, K, V]]
  ): Long => Boolean = {
    val keysSortedAsc = chunksMap.keySet.toList.sorted
    val keyMax        = keysSortedAsc.last
    (key: Long) => key <= (keyMax - 2 * timeWindowMillis)
  }

  override def onChunkRelease(chunk: Chunk[CommittableConsumerRecord[F, K, V]]): F[Unit] = releaseChunk(chunk)
}

object TimeWindowKafkaRecordsAggregatorServiceImpl {

  def make[F[_]: Async, K, V](
    timeWindowMillis: Long,
    releaseChunk:     Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit],
  )(implicit
    logger:           Logger
  ): Resource[F, WindowRecordsAggregatorService[F, Long, CommittableConsumerRecord[F, K, V]]] =
    for {
      chunksRef <- Resource.eval(Async[F].ref(Map.empty[Long, Chunk[CommittableConsumerRecord[F, K, V]]]))
      startRef  <- Resource.eval(Async[F].ref(0L))
      service    = new TimeWindowKafkaRecordsAggregatorServiceImpl(timeWindowMillis, startRef, chunksRef, releaseChunk)
      res       <- WindowRecordsAggregatorService.make(timeWindowMillis * 5, service)
    } yield res

}
