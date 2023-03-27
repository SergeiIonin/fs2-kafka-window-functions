package com.sergeiionin

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref}
import cats.implicits.{catsSyntaxApplicativeId, toFunctorOps}
import cats.syntax.flatMap._
import com.sergeiionin.windowfuncs.windowfuncs.ChunkedRecord
import fs2.Chunk

abstract class CollectorService[F[_] : Async, K, V]() {
  val state: Ref[F, Chunk[ChunkedRecord[F, K, V]]]
  val addCond: ChunkedRecord[F, K, V] => F[Boolean]
  val releaseCond: Chunk[ChunkedRecord[F, K, V]] => F[Boolean]
  val onRelease: Chunk[ChunkedRecord[F, K, V]] => F[Unit]

  private val mutexF = Semaphore.apply(1)

  def addToChunk(cr: ChunkedRecord[F, K, V]): F[Unit] =
    for {
      mutex <- mutexF
      _     <- mutex.acquire
      cond  <- addCond(cr)
      chunk <- state.get
      chunkUpd = if (cond) chunk ++ Chunk(cr) else chunk
      shouldRelease <- releaseCond(chunkUpd)
      update = if (shouldRelease) {
                  Chunk.empty[ChunkedRecord[F, K, V]] -> onRelease(chunkUpd)
               } else {
                  chunkUpd -> ().pure[F]
               }
      _ <- state.modify(_ => update).flatten
      _ <- mutex.release
    } yield ()

/*  def make(addCondP: ChunkedRecord[F, K, V] => Boolean,
           chunkIsFullP: Chunk[ChunkedRecord[F, K, V]] => Boolean,
           onChunkIsFullP: Chunk[ChunkedRecord[F, K, V]] => Unit) =
    Resource.eval(Concurrent[F].ref(Chunk.empty[ChunkedRecord[F, K, V]]).map(s => {
      new CollectorService[F, K, V]() {
        override val state: Ref[F, Chunk[ChunkedRecord[F, K, V]]] = s
        override val addCond: ChunkedRecord[F, K, V] => Boolean = addCondP
        override val chunkIsFull: Chunk[ChunkedRecord[F, K, V]] => Boolean = chunkIsFullP
        override val onChunkIsFull: Chunk[ChunkedRecord[F, K, V]] => Unit = onChunkIsFullP
      }
    }))*/
}

/*
object CollectorService {

  def make[F[_] : Async, K, V](addCond: ChunkedRecord[F, K, V] => Boolean,
                                chunkIsFull: Chunk[ChunkedRecord[F, K, V]] => Boolean,
                  onChunkIsFull: Chunk[ChunkedRecord[F, K, V]] => Unit): Resource[F, CollectorService[F, K, V]] =
    Resource.eval(Concurrent[F].ref(Chunk.empty[ChunkedRecord[F, K, V]]).map(state => {
      new CollectorService(state, addCond, chunkIsFull, onChunkIsFull) {}
  }))

}*/
