package com.sergeiionin.timewindows

import cats.effect.unsafe.implicits.global
import cats.effect.{Async, IO, Ref, Resource}
import com.sergeiionin.Domain.TestRecord
import com.sergeiionin.timewindows.StreamsTimeWindowsAggregatorServiceSpec.StreamTimeWindowAggregatorServiceImpl
import fs2.Chunk
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import wvlet.log.{LogSupport, Logger}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

class StreamsTimeWindowsAggregatorServiceSpec extends AnyFlatSpec with Matchers with LogSupport {

  def log(msg: String): IO[Unit] = IO(logger.info(msg))

  "StreamTimeWindowAggregatorService" should "add coming records into time chunks in real time" in {

    implicit val l: Logger = logger

    val buf = mutable.Map[Long, Chunk[TestRecord]]()

    val timeWindowSizeMillis = 200L

    def onRel(chunk: Chunk[TestRecord]): IO[Unit] = IO {
      if (chunk.nonEmpty) {
        buf.addOne(chunk.head.get.createTime -> chunk)
        ()
      } else
        ()
    }

    var counter = 0

    def emit(): fs2.Stream[IO, TestRecord] = fs2.Stream
      .awakeEvery[IO](20.millis)
      .evalMap(_ => {
        IO(logger.info(s"counter = $counter")) >> IO(counter += 1) >>
        IO(TestRecord("key", "value", System.currentTimeMillis()))
      })
      .interruptAfter(2.second)

    StreamTimeWindowAggregatorServiceImpl
      .make[IO](durationMillis = timeWindowSizeMillis, onRelease = onRel)
      .use(aggregator => {
        val referenceTime = System.currentTimeMillis()
        log(s"reference time = $referenceTime") >>
        emit().evalTap(rec => aggregator.addToChunk(rec)).compile.drain
      })
      .unsafeRunSync()

    val list: List[(Long, List[Long])] = buf.toList.sortBy { case (t, _) => t }.map { case (t, chunk) =>
      t -> chunk.map(_.createTime).toList
    }

    val keys = list.map(_._1)
    keys.sorted shouldBe keys

    println(s"list = ${list.mkString(", \n")}")
    println("")
    println(s"${keys.dropRight(1).zip(keys.drop(1)).map { case (prev, next) => next - prev }.mkString(", \n")}")

    list.foreach { case (_, l) =>
      l.sorted shouldBe l
      val diff = l.last - l.head
      println(s"diff between last and first records timestamps = $diff")
      diff <= timeWindowSizeMillis shouldBe true
    }

    list.foldLeft(0)((next, acc) => next + acc._2.size) shouldBe counter

  }

  "StreamTimeWindowAggregatorService" should "add coming records into time chunks for records which have a time lag" in {
    //
    implicit val l: Logger = logger

    val buf = mutable.Map[Long, Chunk[TestRecord]]()

    val timeWindowSizeMillis = 200L

    val numOfMsgs = 100

    def onRel(chunk: Chunk[TestRecord]): IO[Unit] = IO {
      if (chunk.nonEmpty) {
        buf.addOne(chunk.head.get.createTime -> chunk)
        ()
      } else
        ()
    }

    val stepMillis = 20L

    def recordsChunk(size: Int, stepMillis: Long): Chunk[TestRecord] = {
      val reference = System.currentTimeMillis()
      val list      = (0 until size).toList.map(i => (i, (i * stepMillis) + reference)).map { case (index, time) =>
        TestRecord(s"key-$index", s"value-$index", time)
      }
      Chunk.seq(list)
    }

    def emit(): fs2.Stream[IO, TestRecord] = fs2.Stream.chunk(recordsChunk(numOfMsgs, stepMillis))

    StreamTimeWindowAggregatorServiceImpl
      .make[IO](durationMillis = timeWindowSizeMillis, onRelease = onRel)
      .use(aggregator => {
        val referenceTime = System.currentTimeMillis()
        log(s"reference time = $referenceTime") >>
        emit().evalTap(rec => aggregator.addToChunk(rec)).compile.drain
      })
      .unsafeRunSync()

    val list: List[(Long, List[Long])] = buf.toList.sortBy { case (t, _) => t }.map { case (t, chunk) =>
      t -> chunk.map(_.createTime).toList
    }

    val keys = list.map(_._1)
    keys.sorted shouldBe keys

    println(s"list = ${list.mkString(", \n")}")
    println("")
    println(s"${keys.dropRight(1).zip(keys.drop(1)).map { case (prev, next) => next - prev }.mkString(", \n")}")

    list.foreach { case (_, l) =>
      l.sorted shouldBe l
      val diff = l.last - l.head
      println(s"diff between last and first records timestamps = $diff")
      diff <= timeWindowSizeMillis shouldBe true
    }

    list.foldLeft(0)((next, acc) => next + acc._2.size) shouldBe numOfMsgs

  }

}

object StreamsTimeWindowsAggregatorServiceSpec {

  final case class TimeAndOffset(time: Long, offset: Long)

  class StreamTimeWindowAggregatorServiceImpl[F[_]: Async](
    chunkStateRef:  Ref[F, Map[Long, Chunk[TestRecord]]],
    startRef:       Ref[F, Long],
    durationMillis: Long,
    releaseChunk:   Chunk[TestRecord] => F[Unit],
  )(implicit
    logger:         Logger
  ) extends StreamTimeWindowAggregatorService[F, TestRecord] {
    override val chunkState = chunkStateRef

    override def getStateKey(rec: TestRecord): F[Long] = startRef.modify { start =>
      {
        val recTimestamp = rec.createTime
        val diff         = recTimestamp - start
        val key          = start + (diff / durationMillis) * durationMillis
        start -> key
      }
    }
  }

  object StreamTimeWindowAggregatorServiceImpl {
    def make[F[_]: Async](
      durationMillis: Long,
      onRelease:      Chunk[TestRecord] => F[Unit],
    )(implicit
      logger:         Logger
    ): Resource[F, StreamTimeWindowAggregatorService[F, TestRecord]] = {

      def mainResource(
        chunkStateRef: Ref[F, Map[Long, Chunk[TestRecord]]],
        startRef:      Ref[F, Long],
      ): Resource[F, StreamTimeWindowAggregatorServiceImpl[F]] = Resource.pure(
        new StreamTimeWindowAggregatorServiceImpl(chunkStateRef, startRef, durationMillis, onRelease)
      )

      for {
        chunksRef <- Resource.eval(Async[F].ref(Map.empty[Long, Chunk[TestRecord]]))
        startRef  <- Resource.eval(Async[F].ref(0L))
        _         <- StreamTimeWindowAggregatorService.clearingStreamResource(durationMillis, chunksRef, onRelease)
        main      <- mainResource(chunksRef, startRef)
      } yield main

    }

  }

}
