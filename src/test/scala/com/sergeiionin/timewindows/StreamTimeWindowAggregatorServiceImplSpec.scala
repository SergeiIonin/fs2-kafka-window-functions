package com.sergeiionin.timewindows

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import com.dimafeng.testcontainers.KafkaContainer
import com.sergeiionin.consumer.KafkaConsumerService
import com.sergeiionin.producer.{ProducerService, ProducerServicePlainImpl}
import fs2.Chunk
import fs2.kafka.{CommittableConsumerRecord, commitBatchWithin}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import wvlet.log.{LogSupport, Logger}

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class StreamTimeWindowAggregatorServiceImplSpec extends AnyFlatSpec with Matchers with LogSupport {

  implicit val l: Logger = logger

  def produceEveryNmillis(amountLeft: Int, index: Int, millis: FiniteDuration,
                          producer: ProducerService[IO, String, String],
                          topic: String): IO[Unit] = {
    for {
      rEff <- producer.produce(topic, s"key-$index", s"value-$index")
      r <- rEff
      recs = r.records.toList
      _ <- log(s"records produced are ${recs.map(_._1.key)} -> ${recs.map(_._2.timestamp())}")
      _ <- IO.sleep(millis)
      _ <- IO.whenA(amountLeft != 0)(produceEveryNmillis(amountLeft - 1, index + 1, millis, producer, topic))
    } yield ()
  }

  def log(msg: String): IO[Unit] = IO(logger.info(msg))

  "Chunking Service" should "aggregate records within a timeframe and count the number of records within each frame" in {
    val timeWindowSizeMillis = 200L

    val kafkaContainer = KafkaContainer()

    val kafkaResource: Resource[IO, KafkaContainer] = Resource.make(IO.delay {
      kafkaContainer.start()
      kafkaContainer
    })(container => {
      IO.delay(container.stop())
    })
    val topic = "topic-0"
    val buf = mutable.Map[Long, Chunk[CommittableConsumerRecord[IO, String, String]]]()

    def onRelease(chunk: Chunk[CommittableConsumerRecord[IO, String, String]]): IO[Unit] = IO {
      if (chunk.nonEmpty) {
        buf.addOne(chunk.head.get.record.timestamp.createTime.get -> chunk)
        ()
      } else ()
    }

    /*,
              "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
              "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"*/

    (for {
      kafkaContainer  <- kafkaResource
      props = Map("bootstrap.servers" -> kafkaContainer.bootstrapServers)
      propsProd = props ++ Map("key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")
      propsCons = props ++ Map("group.id" -> "test_group_1", "auto.offset.reset" -> "earliest",
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
      producerService <- ProducerServicePlainImpl.make[IO, String, String](propsProd)
      consumerService <- KafkaConsumerService.make[IO, String, String](propsCons)
      aggregatorService <- StreamTimeWindowAggregatorServiceImpl.make(
        durationMillis = timeWindowSizeMillis, onRelease = onRelease
      )
    } yield (producerService, consumerService, aggregatorService)).use {
      case (producer, consumer, aggregatorService) =>
        produceEveryNmillis(100, 0, 20.millis, producer, topic)
          .racePair(consumer.subscribe(NonEmptyList.one(topic)) >>
            consumer.getStream().evalTap(rec => {
              //log(s"record consumed = ${rec.record.key}") >>
              aggregatorService.addToChunk(rec)
            })
              .map(r => r.offset)
              .through(commitBatchWithin(100, 1.second)).compile.drain)
    }.unsafeRunSync()

    val list = buf.toList.sortBy {
      case (t, _) => t
    }.map {
      case (t, chunk) => t -> chunk.map(_.record.timestamp.createTime.get).toList
    }

    val keys = list.map(_._1)
    keys.sorted shouldBe keys

    println(s"list = ${list.mkString(", \n")}")
    println("")
    println(s"${
      keys.dropRight(1).zip(keys.drop(1)).map {
        case (prev, next) => next - prev
      }.mkString(", \n")
    }")

    list.foreach {
      case (_, l) =>
        l.sorted shouldBe l
        val diff = l.last - l.head
        println(s"diff between last and first records timestamps = $diff")
        diff <= timeWindowSizeMillis shouldBe true
    }

  }

}
