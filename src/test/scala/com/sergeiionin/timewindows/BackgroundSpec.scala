package com.sergeiionin.timewindows

import cats.effect.{IO, Ref, Resource}
import cats.effect.unsafe.implicits.global
import fs2.Chunk
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import wvlet.log.LogSupport

import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

class BackgroundSpec extends AnyFlatSpec with Matchers with LogSupport {

  "another task" should "execute in parallel along with the main task" in {

    val emitInterval  = FiniteDuration(100, MILLISECONDS)
    val checkInterval = FiniteDuration(200, MILLISECONDS)

    IO.ref(Chunk.empty[Int])
      .flatMap { refChunk =>
        {
          (fs2.Stream
            .awakeEvery[IO](emitInterval)
            .evalMap(_ =>
              refChunk.update(chunk => {
                logger.info(s"chunk from main = $chunk")
                chunk ++ Chunk(1)
              })
            )
            .interruptAfter(2.second)
            .compile
            .drain)
            .racePair(
              fs2.Stream
                .awakeEvery[IO](checkInterval)
                .evalMap(_ =>
                  refChunk.update(chunk => {
                    logger.info(s"chunk = $chunk")
                    chunk.map(_ * 10)
                  })
                )
                .interruptAfter(2.second)
                .compile
                .drain
            )
        }
      }
      .unsafeRunSync()
  }

  "background task" should "execute along with the main task" in {

    val emitInterval  = FiniteDuration(100, MILLISECONDS)
    val checkInterval = FiniteDuration(200, MILLISECONDS)

    def backgroundTask(ref: Ref[IO, Chunk[Int]]) = {
      fs2.Stream
        .awakeEvery[IO](checkInterval)
        .evalMap(_ =>
          ref.update(chunk => {
            logger.info(s"chunk = $chunk")
            chunk.map(_ * 10)
          })
        )
        .interruptAfter(2.second)
        .compile
        .drain
    }

    def mainTask(ref: Ref[IO, Chunk[Int]]) = {
      fs2.Stream
        .awakeEvery[IO](emitInterval)
        .evalMap(_ =>
          ref.update(chunk => {
            logger.info(s"chunk from main = $chunk")
            chunk ++ Chunk(1)
          })
        )
        .interruptAfter(2.second)
        .compile
        .drain
    }

    IO.ref(Chunk.empty[Int])
      .flatMap { refChunk =>
        {
          for {
            fib <- backgroundTask(refChunk).start
            _   <- mainTask(refChunk)
            _   <- fib.join
          } yield ()
        }
      }
      .unsafeRunSync()
  }

  "background task with Resource" should "execute along with the main task" in {

    val emitInterval  = FiniteDuration(100, MILLISECONDS)
    val checkInterval = FiniteDuration(200, MILLISECONDS)

    def backgroundTask(ref: Ref[IO, Chunk[Int]]) = {
      fs2.Stream
        .awakeEvery[IO](checkInterval)
        .evalMap(_ =>
          ref.update(chunk => {
            logger.info(s"chunk = $chunk")
            chunk.map(_ * 2)
          })
        )
        // .interruptAfter(2.second)
        .compile
        .drain
        .background
    }

    def mainTask(ref: Ref[IO, Chunk[Int]]) = {
      Resource.eval(
        fs2.Stream
          .awakeEvery[IO](emitInterval)
          .evalMap(_ =>
            ref.update(chunk => {
              logger.info(s"chunk from main = $chunk")
              chunk ++ Chunk(1)
            })
          )
          // .interruptAfter(2.second)
          .compile
          .drain
      )
    }

    IO.ref(Chunk.empty[Int])
      .flatMap { refChunk =>
        {
          val r =
            (for {
              background <- backgroundTask(refChunk)
              main       <- mainTask(refChunk)
            } yield (background, main))
          r.use_
        }
      }
      .unsafeRunSync()
  }

}
