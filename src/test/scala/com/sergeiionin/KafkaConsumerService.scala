package com.sergeiionin

import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.implicits._
import fs2.kafka._
import fs2.{Chunk, Pipe}
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import wvlet.log.Logger

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.MapHasAsScala

class KafkaConsumerService[F[_] : Async, K, V](consumer: KafkaConsumer[F, K, V]) {
  def subscribe(topics: NonEmptyList[String]): F[Unit] = consumer.subscribe(topics)

  def getStream(): fs2.Stream[F, CommittableConsumerRecord[F, K, V]] =
    consumer.partitionedStream.parJoinUnbounded

  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] = consumer.commitSync(offsets)

  def stopConsuming(): F[Unit] = consumer.stopConsuming

  def terminateConsumer(): F[Unit] = consumer.terminate
}

object KafkaConsumerService {
  def make[F[_] : Async, K, V](props: Map[String, String])(implicit deserializerK: Deserializer[F, K],
                               deserializerV: Deserializer[F, V]): Resource[F, KafkaConsumerService[F, K, V]] = {
    val keyDeserializer = Deserializer.apply[F, K]
    val valueDeserializer = Deserializer.apply[F, V]
    val consumerSettings: ConsumerSettings[F, K, V] = ConsumerSettings.apply(keyDeserializer, valueDeserializer).withProperties(props)
    KafkaConsumer.resource(consumerSettings).map(new KafkaConsumerService(_))
  }

  def make[F[_]: Async, K, V](consumerSettings: ConsumerSettings[F, K, V]): Resource[F, KafkaConsumerService[F, K, V]] =
    KafkaConsumer.resource(consumerSettings).map(new KafkaConsumerService(_))
}