package com.sergeiionin

import cats.effect.IO
import cats.effect.kernel.Resource
import com.dimafeng.testcontainers.KafkaContainer
import com.sergeiionin.collectors.CollectorServiceTimeWindowsImpl
import com.sergeiionin.windowfuncs.windowfuncs.ChunkedRecord
import fs2.Chunk
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class WindowFuncsSpec extends AnyFlatSpec with Matchers {

  def produceEveryNmillis(amountLeft: Int, index: Int, millis: FiniteDuration,
                          producer: ProducerService[IO, String, String],
                          topic: String): IO[Unit] = {
    for {
      _ <- producer.produce(topic, s"key-$index", s"value-$index")
      _ <- IO.sleep(millis)
      _ <- IO.whenA(amountLeft != 0)(produceEveryNmillis(amountLeft - 1, index, millis, producer, topic))
    } yield ()
  }

  "Chunking Service" should "aggregate records within a timeframe and count the number of records within each frame" in {
    val kafkaContainer = KafkaContainer()

    val kafkaResource: Resource[IO, KafkaContainer] = Resource.make(IO.delay {
      kafkaContainer.start()
      kafkaContainer
    })(container => {
      IO.delay(container.stop())
    })
    val topic = "topic-0"
    val buf = mutable.Map()[Long, Chunk[ChunkedRecord[IO, String, String]]]

    def onRelease(chunk: Chunk[ChunkedRecord[IO, String, String]]): IO[Unit] = IO{
      if (chunk.nonEmpty) {
        buf + (chunk.head.get.record.timestamp.createTime.get -> chunk)
      } else buf
    }

    (for {
      kafkaContainer  <- kafkaResource
      props           = Map("bootstrap.servers" -> kafkaContainer.bootstrapServers,
                            "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
                            "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")
      producerService <- ProducerServicePlainImpl.make[IO, String, String](props)
      consumerService <- KafkaConsumerService.make[IO, String, String](props)
      collectorService <- CollectorServiceTimeWindowsImpl.make(
        durationMillis = 10L, onRelease = onRelease
      )
      chunkingService <- ChunkingServiceImpl.make(List(collectorService))
    } yield (producerService, consumerService, chunkingService)).use {
      case (producer, consumer, chunkingService) =>
        produceEveryNmillis(100, 0, 10.millis, producer, topic) >>
          consumer.getStream().evalMap(rec => chunkingService.addToChunks(rec)).compile.drain
    }

    val list = buf.toList.sortBy {
      case (k, _) => k
    }.map {
      case (_, chunk) => chunk.map(_.record.key)
    }.zipWithIndex

    println(s"list = $list")

    list.foreach {
      case (recKeys, index) => recKeys shouldBe (index to index * 10).toList.map(i => s"key-$i")
    }

  }

}
