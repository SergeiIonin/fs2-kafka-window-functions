import fs2.Chunk
import fs2.kafka.ProducerRecord
import wvlet.log.Logger

trait ProducerServiceWithBatchScheduling[F[_], K, V] {
  def scheduleToProduce(chunk: Chunk[ProducerRecord[K, V]])(implicit logger: Logger): F[Unit]
}
