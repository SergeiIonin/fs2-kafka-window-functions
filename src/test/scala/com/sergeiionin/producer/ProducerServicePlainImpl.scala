package com.sergeiionin.producer

import cats.effect.kernel.Concurrent
import cats.effect.{Async, Resource}
import fs2.kafka.{KafkaProducer, ProducerSettings, Serializer}

class ProducerServicePlainImpl[F[_]: Async: Concurrent, K, V](kafkaProducer: KafkaProducer[F, K, V])
    extends AbstractProducerService[F, K, V](kafkaProducer)

object ProducerServicePlainImpl {
  def make[F[_]: Async, K, V](
    props:       Map[String, String]
  )(implicit
    serializerK: Serializer[F, K],
    serializerV: Serializer[F, V],
  ): Resource[F, ProducerService[F, K, V]] = {
    val keySerializer    = Serializer.apply[F, K]
    val valueSerializer  = Serializer.apply[F, V]
    val producerSettings = ProducerSettings.apply(keySerializer, valueSerializer).withProperties(props)
    KafkaProducer.resource(producerSettings).map(new ProducerServicePlainImpl(_))
  }
}
