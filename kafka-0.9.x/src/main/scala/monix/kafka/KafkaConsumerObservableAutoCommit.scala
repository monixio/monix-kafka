/*
 * Copyright (c) 2014-2019 by The Monix Project Developers.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.kafka

import monix.eval.Task
import monix.execution.Ack.Stop
import monix.execution.cancelables.BooleanCancelable
import monix.execution.{Ack, Callback}
import monix.kafka.config.ObservableCommitType
import monix.reactive.Observer
import monix.reactive.observers.Subscriber
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

import scala.collection.JavaConverters._
import scala.concurrent.{blocking, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** KafkaConsumerObservable implementation which commits offsets itself.
  */
final class KafkaConsumerObservableAutoCommit[K, V] private[kafka] (
  override protected val config: KafkaConsumerConfig,
  override protected val consumer: Task[KafkaConsumer[K, V]])
    extends KafkaConsumerObservable[K, V, ConsumerRecord[K, V]] {

  /* Based on the [[KafkaConsumerConfig.observableCommitType]] it
   * chooses to commit the offsets in the consumer, by doing either
   * a `commitSync` or a `commitAsync`.
   *
   * MUST BE synchronized by `consumer`.
   */
  private def consumerCommit(consumer: KafkaConsumer[K, V]): Unit =
    config.observableCommitType match {
      case ObservableCommitType.Sync =>
        blocking(consumer.commitSync())
      case ObservableCommitType.Async =>
        blocking(consumer.commitAsync())
    }

  // Caching value to save CPU cycles
  private val pollTimeoutMillis = config.fetchMaxWaitTime.toMillis
  // Boolean value indicating that we should trigger a commit before downstream ack
  private val shouldCommitBefore = !config.enableAutoCommit && config.observableCommitOrder.isBefore
  // Boolean value indicating that we should trigger a commit after downstream ack
  private val shouldCommitAfter = !config.enableAutoCommit && config.observableCommitOrder.isAfter

  override protected def ackTask(consumer: KafkaConsumer[K, V], out: Subscriber[ConsumerRecord[K, V]]): Task[Ack] =
    Task.create { (scheduler, cb) =>
      implicit val s = scheduler
      val asyncCb = Callback.forked(cb)
      val cancelable = BooleanCancelable()

      // Forced asynchronous boundary (on the I/O scheduler)
      s.executeAsync { () =>
        val ackFuture =
          try consumer.synchronized {
            val assignment = consumer.assignment().asScala.toArray
            if (cancelable.isCanceled) Stop
            else {
              consumer.resume(assignment: _ *)
              val next = blocking(consumer.poll(pollTimeoutMillis))
              consumer.pause(assignment: _ *)
              if (shouldCommitBefore) consumerCommit(consumer)
              // Feeding the observer happens on the Subscriber's scheduler
              // if any asynchronous boundaries happen
              isAcked = false
              Observer.feed(out, next.asScala)(out.scheduler)
            }
          } catch {
            case NonFatal(ex) =>
              Future.failed(ex)
          }

        ackFuture.syncOnComplete {
          case Success(ack) =>
            isAcked = true
            // The `streamError` flag protects against contract violations
            // (i.e. onSuccess/onError should happen only once).
            // Not really required, but we don't want to depend on the
            // scheduler implementation.
            var streamErrors = true
            try consumer.synchronized {
              // In case the task has been cancelled, there's no point
              // in continuing to do anything else
              if (cancelable.isCanceled) {
                streamErrors = false
                asyncCb.onSuccess(Stop)
              } else {
                if (shouldCommitAfter) consumerCommit(consumer)
                streamErrors = false
                asyncCb.onSuccess(ack)
              }
            } catch {
              case NonFatal(ex) =>
                if (streamErrors) asyncCb.onError(ex)
                else s.reportFailure(ex)
            }

          case Failure(ex) =>
            isAcked = true
            asyncCb.onError(ex)
        }
      }
      cancelable
    }
}
