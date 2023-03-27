/*
package com.sergeiionin.collectors

import cats.effect.kernel.Concurrent
import cats.effect.std.Semaphore
import cats.effect.{Async, Ref, Resource}
import cats.implicits.catsSyntaxApplicativeId
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.sergeiionin.collectors.EventCountTimeseriesService.{Count, Mutex, StartTime}
import fs2.kafka.{CommittableConsumerRecord, ProducerRecord}
import io.circe.{Json, JsonObject}
import org.joda.time.format.DateTimeFormat

class EventCountTimeseriesService[F[_] : Async : Concurrent](topic: String, chunkSize: Int,
                                                startTime: StartTime,
                                                windowSizeMillis: Long,
                                                refState: Ref[F, Map[Long, Count]],
                                                mutex: Mutex[F],
                                                producer: ProducerServiceWithBatchScheduling[F, Option[String], String]) extends PipedStatisticsService[F](chunkSize, producer) {

 /*
 "start_time" readOnlyKey.window().startTime().toString()
  "end_time" readOnlyKey.window().endTime().toString()
  "window_size" : Int (timeWindows.size())
  "event_count" : Int
  */

  private def getKey(recordTimestamp: Long): Long = {
    val diff = recordTimestamp - startTime
    startTime + (diff / windowSizeMillis) * windowSizeMillis
  }

  private def isRecordObsolete(recordTimestamp: Long) =
    recordTimestamp <= (System.currentTimeMillis() - windowSizeMillis)

  private def updateState(recordTimestamp: Long) =
    refState.getAndUpdate(state => {
      val key = getKey(recordTimestamp)
      if (isRecordObsolete(recordTimestamp)) state
      else {
        val oldCountOpt = state.get(key)
        state.updated(key, oldCountOpt.fold(1L)(count => count + 1))
      }
    })

  private def generateProducerRecord(key: Long): F[Option[ProducerRecord[Option[String], String]]] =
    refState.modify(state => {
      state - key -> {
        val start_time = DateTimeFormat.longTime().print(key)
        val end_time = DateTimeFormat.longTime().print(key + windowSizeMillis)
        val window_size = windowSizeMillis
        val event_count = state.getOrElse(key, 0L)
        Some(ProducerRecord(topic, Some(DateTimeFormat.longTime().print(key)),
          JsonObject("start_time" -> Json.fromString(start_time),
            "end_time" -> Json.fromString(end_time),
            "window_size" -> Json.fromLong(window_size),
            "event_count" -> Json.fromLong(event_count)).toString()
        ))
      }
    })

  private def generateRecord(consumerRecord: CommittableConsumerRecord[F, Option[String], String]) = {
    for {
      _             <- mutex.acquire
      createTimeOpt = consumerRecord.record.timestamp.createTime
      recordOpt     <- if (createTimeOpt.nonEmpty) {
                            val createTime = createTimeOpt.get
                            updateState(createTime).flatMap { _ =>
                              if (isRecordObsolete(createTime)) {
                                val key = getKey(createTime)
                                generateProducerRecord(key)
                              } else None.pure[F]
                            }
                       } else None.pure[F]
      _             <- mutex.release
    } yield recordOpt
  }

  def prepareProducerRecord(initRecord: CommittableConsumerRecord[F, Option[String], String]): F[Option[ProducerRecord[Option[String], String]]] = {
    generateRecord(initRecord)
  }
}

object EventCountTimeseriesService {

  type StartTime = Long
  type Count = Long

  private final case class TimeWindow(start: Long, end: Long)

  final case class Mutex[F[_]](private val sem: Semaphore[F]) {
    def acquire: F[Unit] = sem.acquire
    def release: F[Unit] = sem.release
  }
  object Mutex {
    def make[F[_] : Concurrent]() = Resource.eval(Semaphore[F](1).map(Mutex(_)))
  }

  def make[F[_] : Async : Concurrent](topic: String, chunkSize: Int,
                 startTime: StartTime,
                 windowSizeMillis: Long,
                 producer: ProducerServiceWithBatchScheduling[F, Option[String], String]) = {
    for {
      refState <- Resource.eval(Concurrent[F].ref(Map.empty[Long, Count]))
      mutex    <- Mutex.make()
    } yield new EventCountTimeseriesService(topic, chunkSize, startTime,
      windowSizeMillis, refState, mutex, producer)

  }
}
*/
