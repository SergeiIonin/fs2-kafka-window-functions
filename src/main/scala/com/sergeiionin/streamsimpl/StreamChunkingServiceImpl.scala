package com.sergeiionin.streamsimpl

import cats.effect.{Async, Resource}
import cats.implicits.{toFunctorOps, toTraverseOps}

class StreamChunkingServiceImpl[F[_] : Async, R](collectors: List[StreamCollectorService[F, R]]) extends StreamChunkingService[F, R] {
  override def addToChunks(rec: R): F[Unit] =
    collectors.traverse(_.addToChunk(rec)).void
}

object StreamChunkingServiceImpl {
  def make[F[_] : Async, R](collectors: List[StreamCollectorService[F, R]]): Resource[F, StreamChunkingServiceImpl[F, R]] =
    Resource.pure(new StreamChunkingServiceImpl(collectors))
}
