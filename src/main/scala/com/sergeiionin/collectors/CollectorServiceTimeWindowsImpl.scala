package com.sergeiionin.collectors

import cats.effect.{Async, Ref, Resource}
import cats.implicits.{catsSyntaxApplicativeId, toFlatMapOps, toFunctorOps}
import com.sergeiionin.CollectorService
import com.sergeiionin.windowfuncs.windowfuncs.ChunkedRecord
import fs2.Chunk
import wvlet.log.Logger

class CollectorServiceTimeWindowsImpl[F[_] : Async, K, V](stateRef: Ref[F, Chunk[ChunkedRecord[F, K, V]]],
                                                          timeRef: Ref[F, Long],
                                                          durationMillis: Long,
                                                          releaseChunk: Chunk[ChunkedRecord[F, K, V]] => F[Unit])
                                                         (implicit logger: Logger) extends CollectorService[F, K, V] {
  override val state: Ref[F, Chunk[ChunkedRecord[F, K, V]]] = stateRef

  override val addCond: ChunkedRecord[F, K, V] => F[Boolean] =
    (rec: ChunkedRecord[F, K, V]) => {
      rec.record.timestamp.createTime.fold(false.pure[F])(time =>
      for {
        currentStart <- timeRef.get
        res = time < (currentStart + durationMillis)
      } yield res)
    }
  override val releaseCond: Chunk[ChunkedRecord[F, K, V]] => F[Boolean] =
    _ => timeRef.modify(start => start -> (start + durationMillis < System.currentTimeMillis()))

  private val resetStartTime: F[Unit] = timeRef.update(start => {
    val newStart = System.currentTimeMillis()
    logger.info(s"prev start = $start, newStart = $newStart").pure[F]
    newStart
  }).void

  override val onRelease: Chunk[ChunkedRecord[F, K, V]] => F[Unit] = (chunk: Chunk[ChunkedRecord[F, K, V]]) => {
    for {
      _ <- resetStartTime
      _ <- releaseChunk(chunk)
    } yield ()
  }
}

object CollectorServiceTimeWindowsImpl {

  def make[F[_] : Async, K, V](start: Long = System.currentTimeMillis(),
                               durationMillis: Long,
                               onRelease: Chunk[ChunkedRecord[F, K, V]] => F[Unit]
                              )(implicit logger: Logger): Resource[F, CollectorServiceTimeWindowsImpl[F, K, V]] = {
    Resource.eval(for {
      stateRef   <- Async[F].ref(Chunk.empty[ChunkedRecord[F, K, V]])
      timeRef    <- Async[F].ref(start)
    } yield new CollectorServiceTimeWindowsImpl(stateRef, timeRef, durationMillis, onRelease) {})
  }

}