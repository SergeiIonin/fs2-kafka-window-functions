package com.sergeiionin

import fs2.kafka.CommittableConsumerRecord

trait ChunkingService[F[_], K, V] {

  def addToChunks(cr: CommittableConsumerRecord[F, K, V]): F[Unit]

}
