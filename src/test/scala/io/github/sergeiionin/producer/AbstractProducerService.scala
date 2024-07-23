package io.github.sergeiionin.producer

import cats.effect.Async
import cats.effect.kernel.Concurrent
import fs2.kafka.{KafkaProducer, ProducerResult}

abstract class AbstractProducerService[F[_]: Async: Concurrent, K, V](kafkaProducer: KafkaProducer[F, K, V])
    extends ProducerService[F, K, V] {
  def produce(
    topic: String,
    key:   K,
    value: V,
  ): F[F[ProducerResult[Unit, K, V]]] = kafkaProducer.produceOne(topic, key, value, ())
}
