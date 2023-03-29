package com.sergeiionin.timewindows

import cats.effect.{Async, Ref, Resource}
import cats.implicits.toFunctorOps
import fs2.Chunk
import fs2.kafka.CommittableConsumerRecord
import wvlet.log.Logger
import cats.syntax.flatMap._

// fixme we need to also address the following issues
// 1) the record(s) are coming much later than the current start
// 2) we should better read from each partition to guarantee the order of records and when we may merge the chunks:
// start_time = min(start_time) among all partitions
// end_time = start_time + window_size
// 3) records are coming with the lag and we need to process them as well
// 4) how exactly to merge records from different partitions?
// 5) if the records aren't coming very long, we should set timeRef to 0 so that next record will set it again, then
// how and when to set timeRef to 0?
// 6) also there's a chance that the release action won't be triggered for a very long time

class StreamTimeWindowAggregatorServiceImpl[F[_] : Async, K, V](chunkStateRef: Ref[F, Chunk[CommittableConsumerRecord[F, K, V]]],
                                                     timeRef: Ref[F, Long],
                                                     durationMillis: Long,
                                                     releaseChunk: Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit])
                                                    (implicit logger: Logger) extends StreamTimeWindowAggregatorService[F, CommittableConsumerRecord[F, K, V]]{
  override val chunkState: Ref[F, Chunk[CommittableConsumerRecord[F, K, V]]] = chunkStateRef

  override def addToChunk(rec: CommittableConsumerRecord[F, K, V]): F[Unit] =
    timeRef.update(time => {
      if (time == 0L) rec.record.timestamp.createTime.getOrElse(System.currentTimeMillis()) else time
    }) >> super.addToChunk(rec)

  override def addCond(rec: CommittableConsumerRecord[F, K, V]): F[Boolean] = {
      val timeOpt = rec.record.timestamp.createTime
      for {
        currentStart <- timeRef.get
        res = timeOpt.fold(false)(time => time <= (currentStart + durationMillis))
      } yield res
    }

  override def releaseCond(rec: CommittableConsumerRecord[F, K, V]): F[Boolean] = {
    val timeOpt = rec.record.timestamp.createTime
    for {
      currentStart <- timeRef.get
      res = timeOpt.fold(false)(time => {
        time > (currentStart + durationMillis) && time < (currentStart + 2 * durationMillis)
      })
    } yield res
  }

  override def onRelease(chunk: Chunk[CommittableConsumerRecord[F, K, V]]): F[Unit] =
      timeRef.modify(start => {
        val newStart = start + durationMillis
        newStart -> releaseChunk(chunk)
      }).flatten
}

object StreamTimeWindowAggregatorServiceImpl {

  def make[F[_] : Async, K, V](durationMillis: Long, onRelease: Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit])
                        (implicit logger: Logger): Resource[F, StreamTimeWindowAggregatorServiceImpl[F, K, V]] = {
    Resource.make(for {
      stateRef <- Async[F].ref(Chunk.empty[CommittableConsumerRecord[F, K, V]])
      timeRef <- Async[F].ref(0L)
    } yield new StreamTimeWindowAggregatorServiceImpl(stateRef, timeRef, durationMillis, onRelease) {})(
      service => service.chunkState.modify(chunk => Chunk() -> onRelease(chunk)).flatten
    )
  }

}

