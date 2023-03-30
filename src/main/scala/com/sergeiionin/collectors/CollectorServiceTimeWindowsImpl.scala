package com.sergeiionin.collectors

import cats.effect.{Async, Ref, Resource}
import cats.implicits.{catsSyntaxFlatMapOps, toFlatMapOps, toFunctorOps}
import com.sergeiionin.StreamCollectorService
import fs2.Chunk
import fs2.kafka.CommittableConsumerRecord
import wvlet.log.Logger

class CollectorServiceTimeWindowsImpl[F[_]: Async, K, V](
  stateRef:       Ref[F, Chunk[CommittableConsumerRecord[F, K, V]]],
  timeRef:        Ref[F, Long],
  durationMillis: Long,
  releaseChunk:   Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit],
)(implicit
  logger:         Logger
) extends StreamCollectorService[F, CommittableConsumerRecord[F, K, V]] {
  override val state: Ref[F, Chunk[CommittableConsumerRecord[F, K, V]]] = stateRef

  override def addToChunk(cr: CommittableConsumerRecord[F, K, V]): F[Unit] = {
    timeRef.update(time => {
      if (time == 0L)
        cr.record.timestamp.createTime.getOrElse(System.currentTimeMillis())
      else
        time
    }) >> super.addToChunk(cr)
  }

  //
  override val addCond: CommittableConsumerRecord[F, K, V] => F[Boolean] =
    (rec: CommittableConsumerRecord[F, K, V]) => {
      val timeOpt = rec.record.timestamp.createTime
      for {
        currentStart <- timeRef.get
        res           = timeOpt.fold(false)(time => time < (currentStart + durationMillis))
      } yield res
    }

  override val releaseCond: Chunk[CommittableConsumerRecord[F, K, V]] => F[Boolean] =
    _ => {
      timeRef.modify(start =>
        start -> {
          val diff = System.currentTimeMillis() - start
          logger.info(s"diff = $diff, start = $start")
          diff > durationMillis
        }
      )
    }

  override val onRelease: Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit] =
    (chunk: Chunk[CommittableConsumerRecord[F, K, V]]) => {
      timeRef
        .modify(start => {
          val newStart = start + durationMillis
          newStart -> releaseChunk(chunk)
        })
        .flatten
    }
  //

  /*override val addCond: CommittableConsumerRecord[F, K, V] => F[Boolean] =
    (rec: CommittableConsumerRecord[F, K, V]) => {
      rec.record.timestamp.createTime.fold(false.pure[F])(time =>
      for {
        currentStart <- timeRef.get
        res = time < (currentStart + durationMillis)
      } yield res)
    }
  override val releaseCond: Chunk[CommittableConsumerRecord[F, K, V]] => F[Boolean] =
    _ => timeRef.modify(start => start -> (start + durationMillis < System.currentTimeMillis()))

  private val resetStartTime: F[Unit] = timeRef.update(start => {
    val newStart = System.currentTimeMillis()
    logger.info(s"prev start = $start, newStart = $newStart").pure[F]
    newStart
  }).void

  override val onRelease: Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit] = (chunk: Chunk[CommittableConsumerRecord[F, K, V]]) => {
    for {
      _ <- resetStartTime
      _ <- releaseChunk(chunk)
    } yield ()
  }*/
}

object CollectorServiceTimeWindowsImpl {

  def make[F[_]: Async, K, V](
    durationMillis: Long,
    onRelease:      Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit],
  )(implicit
    logger:         Logger
  ): Resource[F, CollectorServiceTimeWindowsImpl[F, K, V]] = {
    Resource.eval(for {
      stateRef <- Async[F].ref(Chunk.empty[CommittableConsumerRecord[F, K, V]])
      timeRef  <- Async[F].ref(0L)
    } yield new CollectorServiceTimeWindowsImpl(stateRef, timeRef, durationMillis, onRelease) {})
  }

}
