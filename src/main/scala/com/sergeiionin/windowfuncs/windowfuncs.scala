package com.sergeiionin.windowfuncs

import fs2.kafka.CommittableConsumerRecord

package object windowfuncs {

  type ChunkedRecord[F[_], K, V] = CommittableConsumerRecord[F, K, V]

}
