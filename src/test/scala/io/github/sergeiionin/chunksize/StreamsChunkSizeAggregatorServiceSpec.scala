package io.github.sergeiionin.chunksize

import cats.effect.unsafe.implicits.global
import cats.effect.{Async, IO, Ref, Resource}
import cats.syntax.functor._
import io.github.sergeiionin.Domain.TestRecord
import io.github.sergeiionin.WindowRecordsAggregatorService
import io.github.sergeiionin.WindowRecordsAggregatorService.ChunksMap
import io.github.sergeiionin.chunksize.StreamsChunkSizeAggregatorServiceSpec.StreamsChunkSizeAggregatorServiceImpl
import fs2.Chunk
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import wvlet.log.{LogSupport, Logger}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

class StreamsChunkSizeAggregatorServiceSpec extends AnyFlatSpec with Matchers with LogSupport {

  def log(msg: String): IO[Unit] = IO(logger.info(msg))

  "StreamTimeWindowAggregatorService" should "add coming records into time chunks in real time" in {

    implicit val l: Logger = logger

    val buf = mutable.Map[Long, Chunk[TestRecord]]()

    val totalMsgsSize = 100L
    val chunkSize     = 10

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
      .take(totalMsgsSize)

    IO.ref(0)
      .flatMap { ref =>
        StreamsChunkSizeAggregatorServiceImpl
          .make[IO](chunkSize, 500L, onRelease = onRel)
          .use(aggregator => {
            val referenceTime = System.currentTimeMillis()
            log(s"reference time = $referenceTime") >>
            emit(ref).evalTap(rec => aggregator.addToChunk(rec)).compile.drain
          })
      }
      .unsafeRunSync()

    val list: List[(Long, List[Long])] = buf.toList.sortBy { case (t, _) => t }.map { case (t, chunk) =>
      t -> chunk.map(_.createTime).toList
    }

    list.foreach { case (_, l) => l.size shouldBe chunkSize }

    list.foldLeft(0)((next, acc) => next + acc._2.size) shouldBe 100

  }

  "StreamTimeWindowAggregatorService" should "add coming records into time chunks for records which have a time lag" in {
    implicit val l: Logger = logger

    val buf = mutable.Map[Long, Chunk[TestRecord]]()

    val chunkSize = 10

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

    StreamsChunkSizeAggregatorServiceImpl
      .make[IO](chunkSize = chunkSize, 500L, onRelease = onRel)
      .use(aggregator => {
        val referenceTime = System.currentTimeMillis()
        log(s"reference time = $referenceTime") >>
        emit().evalTap(rec => aggregator.addToChunk(rec)).compile.drain
      })
      .unsafeRunSync()

    val list: List[(Long, List[Long])] = buf.toList.sortBy { case (t, _) => t }.map { case (t, chunk) =>
      t -> chunk.map(_.createTime).toList
    }

    list.foreach { case (_, l) => l.size shouldBe chunkSize }

  }

}

object StreamsChunkSizeAggregatorServiceSpec {

  class StreamsChunkSizeAggregatorServiceImpl[F[_]: Async](
    chunkSize:    Int,
    chunksMapRef: Ref[F, Map[Long, Chunk[TestRecord]]],
    releaseChunk: Chunk[TestRecord] => F[Unit],
  )(implicit
    logger:       Logger
  ) extends WindowRecordsAggregatorService[F, Long, TestRecord](chunksMapRef) {

    override def getStateKey(rec: TestRecord): F[Long] = {
      // safe
      def getOrIncrementCurrentKey(currentKey: Long, mapping: ChunksMap[Long, TestRecord]): Long = {
        val size = mapping(currentKey).size
        if (size < chunkSize)
          currentKey
        else
          currentKey + 1
      }

      for {
        chunksMap <- chunksMapRef.get
        key        =
          chunksMap.keySet.toList.sorted match {
            case Nil  => 0L
            case list => getOrIncrementCurrentKey(list.last, chunksMap)
          }
      } yield key
    }

    override def onChunkRelease(chunk: Chunk[TestRecord]): F[Unit] = releaseChunk(chunk)

    override def releaseChunkCondition(chunksMap: ChunksMap[Long, TestRecord]): Long => Boolean = {
      val latestKey = chunksMap.keySet.toList.sorted.max
      (key: Long) => (key != latestKey) || chunksMap.get(key).map(_.size).fold(false)(_ == chunkSize)
    }
  }

  object StreamsChunkSizeAggregatorServiceImpl {
    def make[F[_]: Async](
      chunkSize:        Int,
      timeWindowMillis: Long,
      onRelease:        Chunk[TestRecord] => F[Unit],
    )(implicit
      logger:           Logger
    ): Resource[F, WindowRecordsAggregatorService[F, Long, TestRecord]] =
      for {
        chunksMapRef <- Resource.eval(Async[F].ref(Map(0L -> Chunk.empty[TestRecord])))
        service       = new StreamsChunkSizeAggregatorServiceImpl(chunkSize, chunksMapRef, onRelease)
        main         <- WindowRecordsAggregatorService.make(timeWindowMillis * 2, service)
      } yield main
  }

}
