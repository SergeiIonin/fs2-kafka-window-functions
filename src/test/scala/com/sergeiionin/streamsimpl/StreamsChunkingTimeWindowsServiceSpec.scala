package com.sergeiionin.streamsimpl

import cats.effect.unsafe.implicits.global
import cats.effect.{Async, IO, Ref, Resource}
import cats.implicits.toFunctorOps
import cats.syntax.flatMap._
import com.sergeiionin.streamsimpl.Domain.TestRecord
import com.sergeiionin.streamsimpl.StreamsChunkingTimeWindowsServiceSpec.StreamCollectorServiceTimeWindowsImpl
import fs2.Chunk
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import wvlet.log.{LogSupport, Logger}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

class StreamsChunkingTimeWindowsServiceSpec extends AnyFlatSpec with Matchers with LogSupport {

  "StreamsChunkingService" should "work" in {
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

    val referenceTime = System.currentTimeMillis()

/*    def stream(referenceTime: Long): fs2.Stream[Pure, TestRecord] = {
      val start = referenceTime
      val timestamps = List.range(start, start + 1000L, 10L)
      val records = timestamps.zipWithIndex.map {
        case (time, index) => TestRecord(s"key-$index", s"value-$index", time)
      }
      fs2.Stream.emits(records)
    }*/

    def emit(createTime: Long): fs2.Stream[IO, TestRecord] =
      fs2.Stream.awakeEvery[IO](20.millis)
        .evalMap(time => IO(TestRecord("key", "value", createTime + time.toMillis)))
        .interruptAfter(2.second)

    (for {
      collector <- StreamCollectorServiceTimeWindowsImpl.make[IO](start = referenceTime,
                                                                  durationMillis = 200L, onRelease = onRel)
      //chunking  <- StreamChunkingServiceImpl.make(List(collector))
    } yield (collector)).use {
      case (collector) =>
        val referenceTime = System.currentTimeMillis()
        log(s"reference time = $referenceTime") >>
          collector.setStartTime(referenceTime) >>
            emit(referenceTime).evalTap(rec => collector.addToChunk(rec)).compile.drain
          //stream(referenceTime).evalTap(rec => collector.addToChunk(rec)).compile.drain
    }.unsafeRunSync()

    val list = buf.toList.sortBy {
      case (k, _) => k
    }.map {
      case (_, chunk) => chunk.map(_.createTime)
    }.zipWithIndex

    println(s"buf = $buf")

    println(s"list = ${
      list.map {
        case (chunk, ind) => ind -> chunk.toList.mkString(", ")
      }.mkString(", \n")
    }")

  }

}

object StreamsChunkingTimeWindowsServiceSpec {

  class StreamCollectorServiceTimeWindowsImpl[F[_] : Async](stateRef: Ref[F, Chunk[TestRecord]],
                                                            timeRef: Ref[F, Long],
                                                            durationMillis: Long,
                                                            releaseChunk: Chunk[TestRecord] => F[Unit])
                                                           (implicit logger: Logger) extends StreamCollectorService[F, TestRecord] {
    override val state: Ref[F, Chunk[TestRecord]] = stateRef

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
          logger.info(s"diff = $diff")
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

    def setStartTime(time: Long): F[Unit] = timeRef.set(time)
  }

  object StreamCollectorServiceTimeWindowsImpl {
    def make[F[_] : Async](start: Long, durationMillis: Long, onRelease: Chunk[TestRecord] => F[Unit])
                          (implicit logger: Logger): Resource[F, StreamCollectorServiceTimeWindowsImpl[F]] = {
      Resource.eval(for {
        stateRef <- Async[F].ref(Chunk.empty[TestRecord])
        timeRef <- Async[F].ref(start)
      } yield new StreamCollectorServiceTimeWindowsImpl(stateRef, timeRef, durationMillis, onRelease) {})
    }

  }

}
