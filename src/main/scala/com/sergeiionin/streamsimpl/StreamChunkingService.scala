package com.sergeiionin.streamsimpl

trait StreamChunkingService[F[_], R] {

  def addToChunks(record: R): F[Unit]

}
