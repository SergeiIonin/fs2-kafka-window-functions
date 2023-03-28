package com.sergeiionin.streamsimpl

import cats.effect.unsafe.implicits.global
import cats.effect.{Async, IO, Ref, Resource}
import cats.implicits.toFunctorOps
import cats.syntax.flatMap._
import com.sergeiionin.StreamCollectorService
import com.sergeiionin.streamsimpl.Domain.TestRecord
import com.sergeiionin.streamsimpl.StreamsChunkingTimeWindowsServiceSpec.StreamCollectorServiceTimeWindowsImpl
import fs2.Chunk
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import wvlet.log.{LogSupport, Logger}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

class StreamsChunkingTimeWindowsServiceSpec extends AnyFlatSpec with Matchers with LogSupport {

  "StreamsChunkingService" should "work" in {
//
    implicit val l: Logger = logger

    val buf = mutable.Map[Long, Chunk[TestRecord]]()

    val timeWindowSizeMillis = 200L

    def onRel(chunk: Chunk[TestRecord]): IO[Unit] = IO {
      if (chunk.nonEmpty) {
        buf.addOne(chunk.head.get.createTime -> chunk)
        ()
      } else ()
    }
//
    def log(msg: String): IO[Unit] = IO(logger.info(msg))

    def emit(): fs2.Stream[IO, TestRecord] =
      fs2.Stream.awakeEvery[IO](20.millis)
        .evalMap(_ => IO(TestRecord("key", "value", System.currentTimeMillis())))
        .interruptAfter(2.second)

    (for {
      collector <- StreamCollectorServiceTimeWindowsImpl.make[IO](durationMillis = timeWindowSizeMillis, onRelease = onRel)
      chunking  <- StreamChunkingServiceImpl.make(List(collector))
    } yield chunking).use {
      chunking =>
        val referenceTime = System.currentTimeMillis()
        log(s"reference time = $referenceTime") >>
            emit().evalTap(rec => chunking.addToChunks(rec)).compile.drain
    }.unsafeRunSync()

    val list = buf.toList.sortBy {
      case (t, _) => t
    }.map {
      case (t, chunk) => t -> chunk.map(_.createTime).toList
    }

    list.foreach {
      case (_, l) => {
        l.sorted shouldBe l
        val diff = l.last - l.head
        println(s"diff = $diff")
        diff <= timeWindowSizeMillis shouldBe true
      }
    }

    val keys = list.map(_._1)
    keys.sorted shouldBe keys

  }

}

object StreamsChunkingTimeWindowsServiceSpec {

  final case class TimeAndOffset(time: Long, offset: Long)

  class StreamCollectorServiceTimeWindowsImpl[F[_] : Async](stateRef: Ref[F, Chunk[TestRecord]],
                                                            timeRef: Ref[F, Long],
                                                            durationMillis: Long,
                                                            releaseChunk: Chunk[TestRecord] => F[Unit])
                                                           (implicit logger: Logger) extends StreamCollectorService[F, TestRecord] {
    override val state: Ref[F, Chunk[TestRecord]] = stateRef

    override def addToChunk(rec: TestRecord): F[Unit] = {
      timeRef.update(time => {
        if (time == 0L) rec.createTime else time
      }) >> super.addToChunk(rec)
    }

    override val addCond: TestRecord => F[Boolean] =
      (rec: TestRecord) => {
        val time = rec.createTime
        for {
          currentStart <- timeRef.get
          res = time < (currentStart + durationMillis)
        } yield res
      }

    override val releaseCond: Chunk[TestRecord] => F[Boolean] =
      _ => {
        timeRef.modify(start => start -> {
          val diff = System.currentTimeMillis() - start
          logger.info(s"diff = $diff, start = $start")
          diff > durationMillis
        })
      }

    override val onRelease: Chunk[TestRecord] => F[Unit] = (chunk: Chunk[TestRecord]) => {
      timeRef.modify(start => {
        val newStart = start + durationMillis
        //logger.info(s"prev start = $start, newStart = $newStart").pure[F]
        newStart -> releaseChunk(chunk)
      }).flatten
    }

  }

  object StreamCollectorServiceTimeWindowsImpl {
    def make[F[_] : Async](durationMillis: Long, onRelease: Chunk[TestRecord] => F[Unit])
                          (implicit logger: Logger): Resource[F, StreamCollectorServiceTimeWindowsImpl[F]] = {
      Resource.eval(for {
        stateRef <- Async[F].ref(Chunk.empty[TestRecord])
        timeRef <- Async[F].ref(0L)
      } yield new StreamCollectorServiceTimeWindowsImpl(stateRef, timeRef, durationMillis, onRelease) {})
    }

  }

}
