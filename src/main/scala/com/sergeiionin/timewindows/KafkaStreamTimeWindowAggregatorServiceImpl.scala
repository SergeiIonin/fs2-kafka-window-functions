package com.sergeiionin.timewindows

import cats.effect.implicits.genSpawnOps
import cats.effect.kernel.Spawn
import cats.effect.{Async, Ref, Resource}
import cats.implicits.{catsSyntaxApplicativeId, toTraverseOps}
import cats.syntax.flatMap._
import fs2.Chunk
import fs2.kafka.CommittableConsumerRecord
import wvlet.log.Logger

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

// fixme we need to also address the following issues
// 1) the record(s) are coming much later than the current start
// 2) we should better read from each partition to guarantee the order of records and when we may merge the chunks:
// start_time = min(start_time) among all partitions
// end_time = start_time + window_size
// 3) records are coming with the lag and we need to process them as well
// 4) how exactly to merge records from different partitions?
// 5) if the records aren't coming very long, we should set startRef to 0 so that next record will set it again, then
// how and when to set startRef to 0?
// 6) also there's a chance that the release action won't be triggered for a very long time

// todo add test with plain stream
class KafkaStreamTimeWindowAggregatorServiceImpl[F[_] : Async : Spawn, K, V](chunksRef: Ref[F, Map[Long, Chunk[CommittableConsumerRecord[F, K, V]]]],
                                                                             startRef: Ref[F, Long],
                                                                             durationMillis: Long,
                                                                             releaseChunk: Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit])
                                                                            (implicit logger: Logger) extends StreamTimeWindowAggregatorService[F, CommittableConsumerRecord[F, K, V]]{
  override val chunkState: Ref[F, Map[Long, Chunk[CommittableConsumerRecord[F, K, V]]]] = chunksRef

  override def addToChunk(rec: CommittableConsumerRecord[F, K, V]): F[Unit] =
    startRef.update(time => {
      if (time == 0) rec.record.timestamp.createTime.getOrElse(System.currentTimeMillis()) else time
    }) >> super.addToChunk(rec)

  override def getStateKey(rec: CommittableConsumerRecord[F, K, V]): F[Long] =
    startRef.modify { start => {
        val recTimestamp = rec.record.timestamp.createTime.getOrElse(System.currentTimeMillis())
        val diff = recTimestamp - start
        val key = start + (diff / durationMillis) * durationMillis
        start -> key
      }
    }

}

object KafkaStreamTimeWindowAggregatorServiceImpl {

  def make[F[_] : Async, K, V](durationMillis: Long, onRelease: Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit])
                        (implicit logger: Logger): Resource[F, KafkaStreamTimeWindowAggregatorServiceImpl[F, K, V]] = {

    def clearingStreamResource(chunksRef: Ref[F, Map[Long, Chunk[CommittableConsumerRecord[F, K, V]]]], startRef: Ref[F, Long]) =
      fs2.Stream.awakeEvery(FiniteDuration(durationMillis * 10, MILLISECONDS))
        .evalMap(_ =>
          chunksRef.modify { chunksMap => {
            val keysSortedAsc = chunksMap.keySet.toList.sorted
            val keyLeft = keysSortedAsc.lastOption
            val mapChunksReleasing = keyLeft.fold(Map.empty[Long, Chunk[CommittableConsumerRecord[F, K, V]]])(key => chunksMap.removed(key))
            val chunksMapUpd = keyLeft.fold(Map.empty[Long, Chunk[CommittableConsumerRecord[F, K, V]]])(key => Map(key -> chunksMap(key)))

            chunksMapUpd -> {
              logger.info(s"${System.currentTimeMillis()} in the clearing stream").pure[F]
              mapChunksReleasing.values.toList.traverse(onRelease)
            }
          }
          }.flatten
        ).compile.drain.background
          .onFinalize(logger.info(s"the clearing stream was terminated").pure[F]) // why this message pops up in the beginning???
          .onCancel(Resource.eval(logger.info(s"the clearing stream was canceled").pure[F])) // why this message (also, like the one above) pops up in the beginning???

    def mainResource(chunksRef: Ref[F, Map[Long, Chunk[CommittableConsumerRecord[F, K, V]]]],
                     startRef: Ref[F, Long]): Resource[F, KafkaStreamTimeWindowAggregatorServiceImpl[F, K, V]] =
        Resource.pure(new KafkaStreamTimeWindowAggregatorServiceImpl(chunksRef, startRef, durationMillis, onRelease))

    for {
      chunksRef   <- Resource.eval(Async[F].ref(Map.empty[Long, Chunk[CommittableConsumerRecord[F, K, V]]]))
      startRef    <- Resource.eval(Async[F].ref(0L))
      _          <- clearingStreamResource(chunksRef, startRef)
      main       <- mainResource(chunksRef, startRef)
    } yield main

  }

}

