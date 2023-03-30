package com.sergeiionin

import cats.Parallel
import cats.effect.{Async, Resource}
import cats.implicits.{toFunctorOps, toTraverseOps}
import fs2.kafka.CommittableConsumerRecord

class ChunkingServiceImpl[F[_]: Async: Parallel, K, V](
  collectors: List[StreamCollectorService[F, CommittableConsumerRecord[F, K, V]]]
) extends ChunkingService[F, K, V] {
  override def addToChunks(cr: CommittableConsumerRecord[F, K, V]): F[Unit] = collectors.traverse(_.addToChunk(cr)).void
}

object ChunkingServiceImpl {
  def make[F[_]: Async: Parallel, K, V](
    collectors: List[StreamCollectorService[F, CommittableConsumerRecord[F, K, V]]]
  ): Resource[F, ChunkingServiceImpl[F, K, V]] = Resource.pure(new ChunkingServiceImpl(collectors))
}
