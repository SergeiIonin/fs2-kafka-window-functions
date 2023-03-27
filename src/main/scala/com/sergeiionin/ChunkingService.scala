package com.sergeiionin

import com.sergeiionin.windowfuncs.windowfuncs.ChunkedRecord

trait ChunkingService[F[_], K, V] {

  def addToChunks(cr: ChunkedRecord[F, K, V]): F[Unit]

}
