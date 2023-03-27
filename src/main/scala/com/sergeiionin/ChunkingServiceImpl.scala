package com.sergeiionin

import cats.effect.{Async, Resource}
import cats.implicits.{toFunctorOps, toTraverseOps}
import com.sergeiionin.windowfuncs.windowfuncs.ChunkedRecord
import fs2.kafka.CommittableConsumerRecord

class ChunkingServiceImpl[F[_] : Async, K, V](collectors: List[CollectorService[F, K, V]]) extends ChunkingService[F, K, V] {
  override def addToChunks(cr: ChunkedRecord[F, K, V]): F[Unit] =
    collectors.traverse(_.addToChunk(cr)).void
}

object ChunkingServiceImpl {
  def make[F[_] : Async, K, V](collectors: List[CollectorService[F, K, V]]): Resource[F, ChunkingServiceImpl[F, K, V]] =
    Resource.pure(new ChunkingServiceImpl(collectors))
}
