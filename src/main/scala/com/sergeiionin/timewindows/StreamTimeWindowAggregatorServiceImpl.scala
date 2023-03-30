package com.sergeiionin.timewindows

import cats.effect.implicits.genSpawnOps
import cats.effect.kernel.Spawn
import cats.effect.{Async, Ref, Resource}
import cats.implicits.catsSyntaxApplicativeId
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
// 5) if the records aren't coming very long, we should set timeRef to 0 so that next record will set it again, then
// how and when to set timeRef to 0?
// 6) also there's a chance that the release action won't be triggered for a very long time

class StreamTimeWindowAggregatorServiceImpl[F[_] : Async : Spawn, K, V](chunkStateRef: Ref[F, Chunk[CommittableConsumerRecord[F, K, V]]],
                                                                        timeRef: Ref[F, Long],
                                                                        durationMillis: Long,
                                                                        releaseChunk: Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit])
                                                                       (implicit logger: Logger) extends StreamTimeWindowAggregatorService[F, CommittableConsumerRecord[F, K, V]]{
  override val chunkState: Ref[F, Chunk[CommittableConsumerRecord[F, K, V]]] = chunkStateRef

  override def addToChunk(rec: CommittableConsumerRecord[F, K, V]): F[Unit] =
    timeRef.update(time => {
      if (time % durationMillis == 0) rec.record.timestamp.createTime.getOrElse(System.currentTimeMillis()) else time
    }) >> super.addToChunk(rec)

  override def addCond(rec: CommittableConsumerRecord[F, K, V]): F[Boolean] = {
      val timeOpt = rec.record.timestamp.createTime
      timeRef.modify(currentStart => {
        currentStart -> {
          timeOpt.fold(false)(time => {
            val cond = time <= (currentStart + durationMillis)
            logger.info(s"time for rec = $time, currentStart = $currentStart, durationMillis = $durationMillis, cond = $cond")
            cond
          })
        }
      })
    }

/*  def releaseChunkInBackground(): F[Unit] =
    Async[F].background(chunkState.modify(chunk =>
      Chunk() -> {
        Async[F].whenA(chunk.nonEmpty) {
          logger.info(s"releasing the chunk after $durationMillis has elapsed").pure[F] >>
            Async[F].sleep(FiniteDuration.apply(durationMillis, MILLISECONDS)) >>
              onRelease(chunk)
        }
      }
    ).flatten).use(_ => {
      ().pure[F]
    })*/

}

object StreamTimeWindowAggregatorServiceImpl {

  def make[F[_] : Async, K, V](durationMillis: Long, onRelease: Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit])
                        (implicit logger: Logger): Resource[F, StreamTimeWindowAggregatorServiceImpl[F, K, V]] = {

    def updateStartTimeAndReleaseChunk(timeRef: Ref[F, Long], chunk: Chunk[CommittableConsumerRecord[F, K, V]]): F[Unit] =
      timeRef.modify { start => {
          val newStart = start + durationMillis
        logger.info(s"will upd start to $newStart")
          newStart -> onRelease(chunk)
        }
      }.flatten

    def clearingStreamResource(stateRef: Ref[F, Chunk[CommittableConsumerRecord[F, K, V]]], timeRef: Ref[F, Long]) =
      fs2.Stream.awakeEvery(FiniteDuration(durationMillis, MILLISECONDS))
        .evalMap(_ =>
          stateRef.modify { chunk => {
            Chunk() -> {
              logger.info(s"${System.currentTimeMillis()} in the clearing stream").pure[F]
              updateStartTimeAndReleaseChunk(timeRef, chunk)
            }
          }
          }.flatten
        ).compile.drain.background
          .onFinalize(logger.info(s"the clearing stream was terminated").pure[F]) // why this message pops up in the beginning???
          .onCancel(Resource.eval(logger.info(s"the clearing stream was canceled").pure[F])) // why this message (also, like the one above) pops up in the beginning???

    def mainResource(stateRef: Ref[F, Chunk[CommittableConsumerRecord[F, K, V]]],
                     timeRef: Ref[F, Long]): Resource[F, StreamTimeWindowAggregatorServiceImpl[F, K, V]] =
        Resource.pure(new StreamTimeWindowAggregatorServiceImpl(stateRef, timeRef, durationMillis, onRelease))

/*
    def startStreamOrFail(out: Outcome[F, Throwable, fs2.Stream[F, Unit]]): F[Unit] = out match {
      case Succeeded(f) => f.flatMap(_.compile.drain)
      case Canceled() => Async[F].raiseError(new RuntimeException("Can't start background task!"))
      case Errored(e) => Async[F].raiseError(e)
    }
*/

    for {
      stateRef   <- Resource.eval(Async[F].ref(Chunk.empty[CommittableConsumerRecord[F, K, V]]))
      timeRef    <- Resource.eval(Async[F].ref(0L))
      _          <- clearingStreamResource(stateRef, timeRef)
      main       <- mainResource(stateRef, timeRef)
    } yield main
    /*StreamTimeWindowAggregatorServiceImpl(stateRef, timeRef, durationMillis, onRelease) {}
    ) (
      service => service.chunkState.modify(chunk => Chunk() -> onRelease(chunk)).flatten
      )*/

  }

}

