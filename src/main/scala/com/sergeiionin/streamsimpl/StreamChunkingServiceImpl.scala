package com.sergeiionin.streamsimpl

import cats.Parallel
import cats.effect.{Async, Resource}
import cats.implicits.{catsSyntaxParallelTraverse1, toFunctorOps}
import com.sergeiionin.StreamCollectorService

class StreamChunkingServiceImpl[F[_] : Async : Parallel, R](collectors: List[StreamCollectorService[F, R]]) extends StreamChunkingService[F, R] {
  override def addToChunks(rec: R): F[Unit] =
    collectors.parTraverse(_.addToChunk(rec)).void
}

object StreamChunkingServiceImpl {
  def make[F[_] : Async : Parallel, R](collectors: List[StreamCollectorService[F, R]]): Resource[F, StreamChunkingServiceImpl[F, R]] =
    Resource.pure(new StreamChunkingServiceImpl(collectors))
}
