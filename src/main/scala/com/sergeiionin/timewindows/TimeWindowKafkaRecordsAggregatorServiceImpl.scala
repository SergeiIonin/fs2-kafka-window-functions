package com.sergeiionin.timewindows

import cats.effect.{Async, Ref, Resource}
import cats.implicits.catsSyntaxFlatMapOps
import cats.syntax.functor._
import com.sergeiionin.WindowRecordsAggregatorService
import com.sergeiionin.WindowRecordsAggregatorService.ChunksMap
import fs2.Chunk
import fs2.kafka.CommittableConsumerRecord
import wvlet.log.Logger

// fixme we need to also address the following issues
// 1) the record(s) are coming much later than the current start => We don't need to operate with current time,
// instead we need to regularly clean chunks of msgs
// 2) we should better read from each partition to guarantee the order of records and when we may merge the chunks:
// start_time = min(start_time) among all partitions
// end_time = start_time + window_size
// 3) records are coming with the lag and we need to process them as well
// 4) how exactly to merge records from different partitions?
// 5) if the records aren't coming very long, we should set startRef to 0 so that next record will set it again, then
// how and when to set startRef to 0?
// 6) also there's a chance that the release action won't be triggered for a very long time => should regularly clean chunks of msgs
// via background task (stream)

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
