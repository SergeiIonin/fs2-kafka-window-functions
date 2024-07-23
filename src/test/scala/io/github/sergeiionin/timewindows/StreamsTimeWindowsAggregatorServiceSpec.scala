package io.github.sergeiionin.timewindows

import cats.effect.unsafe.implicits.global
import cats.effect.{Async, IO, Ref, Resource}
import io.github.sergeiionin.Domain.TestRecord
import io.github.sergeiionin.WindowRecordsAggregatorService
import io.github.sergeiionin.WindowRecordsAggregatorService.ChunksMap
import io.github.sergeiionin.timewindows.StreamsTimeWindowsAggregatorServiceSpec.StreamTimeWindowAggregatorServiceImpl
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

    val numOfMsgs = 100L

    def onRel(chunk: Chunk[TestRecord]): IO[Unit] = IO {
      if (chunk.nonEmpty) {
        buf.addOne(chunk.head.get.createTime -> chunk)
        ()
      } else
        ()
    }

    def emit(counter: Ref[IO, Int]): fs2.Stream[IO, TestRecord] = fs2.Stream
      .awakeEvery[IO](20.millis)
      .evalMap(_ => {
        counter.updateAndGet(_ + 1).flatMap(ctr => log(s"counter = $ctr")) >>
        IO(TestRecord("key", "value", System.currentTimeMillis()))
      })
      .take(numOfMsgs)

    IO.ref(0)
      .flatMap { counter =>
        StreamTimeWindowAggregatorServiceImpl
          .make[IO](timeWindowMillis = timeWindowSizeMillis, onRelease = onRel)
          .use(aggregator => {
            val referenceTime = System.currentTimeMillis()
            log(s"reference time = $referenceTime") >>
            emit(counter).evalTap(rec => aggregator.addToChunk(rec)).compile.drain
          })
      }
      .unsafeRunSync()

    val list: List[(Long, List[Long])] = buf.toList.sortBy { case (t, _) => t }.map { case (t, chunk) =>
      t -> chunk.map(_.createTime).toList
    }

    val keys = list.map(_._1)
    keys.sorted shouldBe keys

    list.foreach { case (_, l) =>
      l.sorted shouldBe l
      val diff = l.last - l.head
      diff <= timeWindowSizeMillis shouldBe true
    }

    list.foldLeft(0)((next, acc) => next + acc._2.size) shouldBe numOfMsgs

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
      .make[IO](timeWindowMillis = timeWindowSizeMillis, onRelease = onRel)
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

    list.foreach { case (_, l) =>
      l.sorted shouldBe l
      val diff = l.last - l.head
      diff <= timeWindowSizeMillis shouldBe true
    }

    list.foldLeft(0)((next, acc) => next + acc._2.size) shouldBe numOfMsgs

  }

}

object StreamsTimeWindowsAggregatorServiceSpec {

  final case class TimeAndOffset(time: Long, offset: Long)

  class StreamTimeWindowAggregatorServiceImpl[F[_]: Async](
    chunkStateRef:    Ref[F, Map[Long, Chunk[TestRecord]]],
    startRef:         Ref[F, Long],
    timeWindowMillis: Long,
    releaseChunk:     Chunk[TestRecord] => F[Unit],
  )(implicit
    logger:           Logger
  ) extends WindowRecordsAggregatorService[F, Long, TestRecord](chunkStateRef) {

    override def getStateKey(rec: TestRecord): F[Long] = startRef.modify { start =>
      {
        val recTimestamp = rec.createTime
        val diff         = recTimestamp - start
        val key          = start + (diff / timeWindowMillis) * timeWindowMillis
        start -> key
      }
    }

    override def onChunkRelease(chunk: Chunk[TestRecord]): F[Unit] = releaseChunk(chunk)

    override def releaseChunkCondition(chunksMap: ChunksMap[Long, TestRecord]): Long => Boolean = {
      val keysSortedAsc = chunksMap.keySet.toList.sorted
      val keyMax        = keysSortedAsc.last
      (key: Long) => key <= (keyMax - 2 * timeWindowMillis)
    }
  }

  object StreamTimeWindowAggregatorServiceImpl {
    def make[F[_]: Async](
      timeWindowMillis: Long,
      onRelease:        Chunk[TestRecord] => F[Unit],
    )(implicit
      logger:           Logger
    ): Resource[F, WindowRecordsAggregatorService[F, Long, TestRecord]] =
      for {
        chunkStateRef <- Resource.eval(Async[F].ref(Map.empty[Long, Chunk[TestRecord]]))
        startRef      <- Resource.eval(Async[F].ref(0L))
        service        = new StreamTimeWindowAggregatorServiceImpl(chunkStateRef, startRef, timeWindowMillis, onRelease)
        main          <- WindowRecordsAggregatorService.make(timeWindowMillis * 2, service)
      } yield main
  }

}
