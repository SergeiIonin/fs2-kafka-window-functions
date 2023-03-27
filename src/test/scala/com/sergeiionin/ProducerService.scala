package com.sergeiionin

import fs2.kafka.ProducerResult

trait ProducerService[F[_], K, V] {
  def produce(topic: String, key: K, value: V): F[F[ProducerResult[Unit, K, V]]]
}
