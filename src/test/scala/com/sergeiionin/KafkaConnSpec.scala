package com.sergeiionin

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.sergeiionin.collectors.CollectorServiceTimeWindowsImpl
import com.sergeiionin.consumer.KafkaConsumerService
import fs2.Chunk
import fs2.kafka.{CommittableConsumerRecord, commitBatchWithin}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import wvlet.log.{LogSupport, Logger}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

class KafkaConnSpec extends AnyFlatSpec with Matchers with LogSupport {

  implicit val l: Logger = logger

  def log(msg: String): IO[Unit] = IO(logger.info(msg))

  "Chunking Service" should "aggregate records within a timeframe and count the number of records within each frame" in {

    val topic = "topic-0"
    val buf = mutable.Map[Long, Chunk[CommittableConsumerRecord[IO, String, String]]]()

    def onRelease(chunk: Chunk[CommittableConsumerRecord[IO, String, String]]): IO[Unit] = IO {
      if (chunk.nonEmpty) {
        buf.addOne(chunk.head.get.record.timestamp.createTime.get -> chunk)
        ()
      } else ()
    }

    val props = Map("bootstrap.servers" -> "PLAINTEXT://localhost:9092")
    val propsCons = props ++
      Map("group.id" -> "test_group_3",
          "auto.offset.reset" -> "earliest",
          "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
          "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")

    (for {
      consumerService <- KafkaConsumerService.make[IO, String, String](propsCons)
      collectorService <- CollectorServiceTimeWindowsImpl.make(
        durationMillis = 100L, onRelease = onRelease
      )
      chunkingService <- ChunkingServiceImpl.make(List(collectorService))
    } yield (consumerService, chunkingService)).use {
        case (consumer, chunkingService) =>
          consumer.subscribe(NonEmptyList.one(topic)) >>
            consumer.getStream().evalTap(rec => {
              log(s"record consumed = ${rec.record.key}") >>
                chunkingService.addToChunks(rec)
            })
              .map(r => r.offset)
              .through(commitBatchWithin(100, 1.second)).compile.drain
    }.unsafeRunSync()

    ///
    val list = buf.toList.sortBy {
      case (k, _) => k
    }.map {
      case (_, chunk) => chunk.map(_.record.timestamp.createTime.get)
    }.zipWithIndex

    println(s"buf = $buf")

    println(s"list = ${list.map {
      case (chunk, ind) => ind -> chunk.toList.mkString(", ")
    }.mkString(", \n")}")

    list.foreach {
      case (recKeys, index) => recKeys shouldBe (index to index * 10).toList.map(i => s"key-$i")
    }

  }

}
