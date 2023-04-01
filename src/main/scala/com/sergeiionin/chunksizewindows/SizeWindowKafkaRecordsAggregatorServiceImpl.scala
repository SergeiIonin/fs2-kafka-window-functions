package com.sergeiionin.timewindows

import cats.effect.{Async, Ref, Resource}
import cats.implicits.catsSyntaxFlatMapOps
import com.sergeiionin.WindowRecordsAggregatorService
import com.sergeiionin.WindowRecordsAggregatorService.ChunksMap
import fs2.Chunk
import fs2.kafka.CommittableConsumerRecord
import wvlet.log.Logger

class TimeWindowKafkaRecordsAggregatorServiceImpl[F[_] : Async, K, V]
      (timeWindowMillis: Long, startRef: Ref[F, Long],
       chunksRef: Ref[F, ChunksMap[CommittableConsumerRecord[F, K, V]]],
       releaseChunk: Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit])(implicit logger: Logger) extends WindowRecordsAggregatorService[F, CommittableConsumerRecord[F, K, V]](chunksRef) {

  override def addToChunk(rec: CommittableConsumerRecord[F, K, V]): F[Unit] =
    startRef.update(time => {
      if (time == 0)
        rec.record.timestamp.createTime.getOrElse(System.currentTimeMillis())
      else
        time
    }) >> super.addToChunk(rec)

  override def getStateKey(rec: CommittableConsumerRecord[F, K, V]): F[Long] = startRef.modify { start => {
      val recTimestamp = rec.record.timestamp.createTime.getOrElse(System.currentTimeMillis())
      val diff = recTimestamp - start
      val key = start + (diff / timeWindowMillis) * timeWindowMillis
      start -> key
    }
  }

  override def splitChunks(chunksMap: ChunksMap[CommittableConsumerRecord[F, K, V]]): (ChunksMap[CommittableConsumerRecord[F, K, V]], ChunksMap[CommittableConsumerRecord[F, K, V]]) = {
    if (chunksMap.nonEmpty) {
      val keysSortedAsc = chunksMap.keySet.toList.sorted
      val keyMax = keysSortedAsc.last
      val keysReleasing = keysSortedAsc.filter(_ <= (keyMax - 2 * timeWindowMillis)) // todo should be configurable
      val keysLeft = keysSortedAsc diff keysReleasing

      val mapChunksReleasing = chunksMap.removedAll(keysLeft)
      val mapChunksLeft = chunksMap.removedAll(keysReleasing)

      mapChunksLeft -> mapChunksReleasing
    } else Map.empty[Long, Chunk[CommittableConsumerRecord[F, K, V]]] ->
              Map.empty[Long, Chunk[CommittableConsumerRecord[F, K, V]]]
  }

  override def onChunkRelease(chunk: Chunk[CommittableConsumerRecord[F, K, V]]): F[Unit] = releaseChunk(chunk)
}

object TimeWindowKafkaRecordsAggregatorServiceImpl {

  def make[F[_] : Async, K, V](timeWindowMillis: Long, releaseChunk: Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit])
                              (implicit logger: Logger): Resource[F, WindowRecordsAggregatorService[F, CommittableConsumerRecord[F, K, V]]] =
    for {
      chunksRef <- Resource.eval(Async[F].ref(Map.empty[Long, Chunk[CommittableConsumerRecord[F, K, V]]]))
      startRef  <- Resource.eval(Async[F].ref(0L))
      service   = new TimeWindowKafkaRecordsAggregatorServiceImpl(timeWindowMillis, startRef, chunksRef, releaseChunk)
      res       <- WindowRecordsAggregatorService.make(timeWindowMillis * 5, service)
    } yield res

}
