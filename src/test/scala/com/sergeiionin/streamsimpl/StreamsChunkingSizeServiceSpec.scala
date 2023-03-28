package com.sergeiionin.streamsimpl

import cats.effect.unsafe.implicits.global
import cats.effect.{Async, IO, Ref, Resource}
import cats.implicits.{catsSyntaxApplicativeId, toFunctorOps}
import com.sergeiionin.streamsimpl.Domain.TestRecord
import com.sergeiionin.streamsimpl.StreamsChunkingSizeServiceSpec.StreamCollectorServiceChunkSizeImpl
import fs2.Chunk
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import wvlet.log.{LogSupport, Logger}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

class StreamsChunkingSizeServiceSpec extends AnyFlatSpec with Matchers with LogSupport {

  "StreamsChunkingSizeService" should "work" in {
//
    implicit val l: Logger = logger

    val buf = mutable.Map[Long, Chunk[TestRecord]]()

    def onRel(chunk: Chunk[TestRecord]): IO[Unit] = IO {
      if (chunk.nonEmpty) {
        buf.addOne(chunk.head.get.createTime -> chunk)
        ()
      } else ()
    }
//
    def log(msg: String): IO[Unit] = IO(logger.info(msg))

    def emit(createTime: Long): fs2.Stream[IO, TestRecord] =
      fs2.Stream.awakeEvery[IO](20.millis)
        .evalMap(time => IO(TestRecord("key", "value", createTime + time.toMillis)))
        .interruptAfter(2.second)

    (for {
      collector <- StreamCollectorServiceChunkSizeImpl.make[IO](size = 10, onRelease = onRel)
      //chunking  <- StreamChunkingServiceImpl.make(List(collector))
    } yield (collector)).use {
      case (collector) =>
        val referenceTime = System.currentTimeMillis()
        log(s"reference time = $referenceTime") >>
            emit(referenceTime).evalTap(rec => collector.addToChunk(rec)).compile.drain
    }.unsafeRunSync()

    val list = buf.toList.sortBy {
      case (t, _) => t
    }.map {
      case (t, chunk) => t -> chunk.map(_.createTime).toList
    }

    list.foreach {
      case (_, list) => list.sorted shouldBe list
    }

    val keys = list.map(_._1)
    keys.sorted shouldBe keys

    println(s"list = $list")

  }

}

object StreamsChunkingSizeServiceSpec {

  class StreamCollectorServiceChunkSizeImpl[F[_] : Async](stateRef: Ref[F, Chunk[TestRecord]],
                                                          size: Int,
                                                          releaseChunk: Chunk[TestRecord] => F[Unit])
                                                          (implicit logger: Logger) extends StreamCollectorService[F, TestRecord] {
    override val state: Ref[F, Chunk[TestRecord]] = stateRef

    override val addCond: TestRecord => F[Boolean] =
      _ => stateRef.modify(chunk => chunk -> (chunk.size < size))

    override val releaseCond: Chunk[TestRecord] => F[Boolean] =
      chunk => (chunk.size >= size).pure[F]

    override val onRelease: Chunk[TestRecord] => F[Unit] =
      releaseChunk
  }

  object StreamCollectorServiceChunkSizeImpl {
    def make[F[_] : Async](size: Int, onRelease: Chunk[TestRecord] => F[Unit])
                          (implicit logger: Logger): Resource[F, StreamCollectorServiceChunkSizeImpl[F]] =
      Resource.eval(
        Async[F].ref(Chunk.empty[TestRecord]).map(stateRef =>
          new StreamCollectorServiceChunkSizeImpl(stateRef, size, onRelease) {})
      )
  }

}

