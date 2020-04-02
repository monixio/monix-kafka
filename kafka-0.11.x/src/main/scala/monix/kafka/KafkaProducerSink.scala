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

import com.typesafe.scalalogging.StrictLogging
import monix.eval.{Coeval, Task}
import monix.execution.Ack.{Continue, Stop}
import monix.execution.cancelables.AssignableCancelable
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

import scala.concurrent.Future
import scala.util.{Failure, Success}

/** A `monix.reactive.Consumer` that pushes incoming messages into
  * a [[KafkaProducer]].
  */
final class KafkaProducerSink[K, V] private (
  producer: Coeval[KafkaProducer[K, V]],
  shouldTerminate: Boolean,
  parallelism: Int)
    extends Consumer[Seq[ProducerRecord[K, V]], Unit] with StrictLogging with Serializable {

  require(parallelism >= 1, "parallelism >= 1")

  def createSubscriber(
    cb: Callback[Throwable, Unit],
    s: Scheduler): (Subscriber[Seq[ProducerRecord[K, V]]], AssignableCancelable.Multi) = {
    val out = new Subscriber[Seq[ProducerRecord[K, V]]] { self =>
      implicit val scheduler: Scheduler = s
      private[this] val p = producer.memoize
      private[this] var isActive = true

      def onNext(list: Seq[ProducerRecord[K, V]]): Future[Ack] =
        self.synchronized {
          if (!isActive) Stop
          else {
            val sendTask: Task[Seq[Option[RecordMetadata]]] =
              if (parallelism == 1)
                Task.traverse(list)(p.value().send(_))
              else
                Task.wanderN(parallelism)(list)(r => p.value().send(r))

            val recovered = sendTask.redeem(
              ex => {
                logger.error("Unexpected error in KafkaProducerSink", ex)
                Continue
              },
              _ => Continue
            )

            recovered.runToFuture
          }
        }

      def terminate(cb: => Unit): Unit =
        self.synchronized {
          if (isActive) {
            isActive = false

            if (!shouldTerminate) cb
            else
              Task(p.value.close()).flatten.materialize.foreach {
                case Success(_) => cb
                case Failure(ex) =>
                  logger.error("Unexpected error in KafkaProducerSink", ex)
                  cb
              }
          }
        }

      def onError(ex: Throwable): Unit =
        terminate(cb.onError(ex))
      def onComplete(): Unit =
        terminate(cb.onSuccess(()))
    }

    (out, AssignableCancelable.dummy)
  }
}

object KafkaProducerSink {

  /** Builder for [[KafkaProducerSink]]. */
  def apply[K, V](config: KafkaProducerConfig, sc: Scheduler)(
    implicit K: Serializer[K],
    V: Serializer[V]): KafkaProducerSink[K, V] = {

    val producer = Coeval(KafkaProducer[K, V](config, sc))
    new KafkaProducerSink(producer, shouldTerminate = true, parallelism = config.monixSinkParallelism)
  }

  /** Builder for [[KafkaProducerSink]]. */
  def apply[K, V](producer: Coeval[KafkaProducer[K, V]], parallelism: Int): KafkaProducerSink[K, V] =
    new KafkaProducerSink(producer, shouldTerminate = false, parallelism = parallelism)
}
