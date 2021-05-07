/*
 * Copyright (c) 2014-2021 by The Monix Project Developers.
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
import monix.execution.Ack.{Continue, Stop}
import monix.execution.{Ack, Callback, Cancelable, Scheduler}
import monix.kafka.config.ObservableCommitOrder
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, KafkaConsumer}

import scala.jdk.CollectionConverters._
import scala.concurrent.blocking
import scala.util.matching.Regex
import scala.concurrent.duration._

/** Exposes an `Observable` that consumes a Kafka stream by
  * means of a Kafka Consumer client.
  *
  * In order to get initialized, it needs a configuration. See the
  * [[KafkaConsumerConfig]] needed and see `monix/kafka/default.conf`,
  * (in the resource files) that is exposing all default values.
  */
trait KafkaConsumerObservable[K, V, Out] extends Observable[Out] {

  protected def config: KafkaConsumerConfig
  protected def consumerT: Task[Consumer[K, V]]

  @volatile
  protected var isAcked = true

  /** Creates a task that polls the source, then feeds the downstream
    * subscriber, returning the resulting acknowledgement
    */
  protected def ackTask(consumer: Consumer[K, V], out: Subscriber[Out]): Task[Ack]

  override final def unsafeSubscribeFn(out: Subscriber[Out]): Cancelable = {
    import out.scheduler

    val callback = new Callback[Throwable, Unit] {
      def onSuccess(value: Unit): Unit =
        out.onComplete()

      def onError(ex: Throwable): Unit =
        out.onError(ex)
    }

    feedTask(out).runAsync(callback)
  }

  private def feedTask(out: Subscriber[Out]): Task[Unit] = {
    Task.create { (scheduler, cb) =>
      implicit val s = scheduler
      val startConsuming =
        consumerT.bracket { c =>
          // Skipping all available messages on all partitions
          if (config.observableSeekOnStart.isSeekEnd) c.seekToEnd(Nil.asJavaCollection)
          else if (config.observableSeekOnStart.isSeekBeginning) c.seekToBeginning(Nil.asJavaCollection)
          Task.race(runLoop(c, out), pollHeartbeat(c).loopForever).void
        } { consumer =>
          // Forced asynchronous boundary
          Task.evalAsync(consumer.synchronized(blocking(consumer.close())))
        }
      startConsuming.runAsync(cb)
    }
  }

  /** Returns a task that continuously polls the `KafkaConsumer` for
    * new messages and feeds the given subscriber.
    *
    * Creates an asynchronous boundary on every poll.
    */
  private def runLoop(consumer: Consumer[K, V], out: Subscriber[Out]): Task[Unit] = {
    ackTask(consumer, out).flatMap {
      case Stop => Task.unit
      case Continue => runLoop(consumer, out)
    }
  }

  /** Returns task that constantly polls the `KafkaConsumer` in case subscriber
    * is still processing last fed batch.
    * This allows producer process commit calls and also keeps consumer alive even
    * with long batch processing.
    *
    * If polling fails the error is reported to the subscriber through the scheduler.
    *
    * @see [[https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread]]
    */
  private def pollHeartbeat(consumer: Consumer[K, V])(implicit scheduler: Scheduler): Task[Unit] = {
    Task.sleep(config.pollHeartbeatRate) >>
      Task.eval {
        if (!isAcked) {
          consumer.synchronized {
            // needed in order to ensure that the consumer assignment
            // is paused, meaning that no messages will get lost.
            val assignment = consumer.assignment()
            consumer.pause(assignment)
            val records = blocking(consumer.poll(0))
            if (!records.isEmpty) {
              val errorMsg = s"Received ${records.count()} unexpected messages."
              throw new IllegalStateException(errorMsg)
            }
          }
        }
      }.onErrorHandleWith { ex =>
        Task.now(scheduler.reportFailure(ex)) >>
          Task.sleep(1.seconds)
      }
  }

}

object KafkaConsumerObservable {

  /** Builds a [[KafkaConsumerObservable]] instance.
    *
    * @param cfg      is the [[KafkaConsumerConfig]] needed for initializing the
    *                 consumer; also make sure to see `monix/kafka/default.conf` for
    *                 the default values being used.
    * @param consumer is a factory for the
    *                 `org.apache.kafka.clients.consumer.KafkaConsumer`
    *                 instance to use for consuming from Kafka
    */
  def apply[K, V](
    cfg: KafkaConsumerConfig,
    consumer: Task[Consumer[K, V]]): KafkaConsumerObservable[K, V, ConsumerRecord[K, V]] =
    new KafkaConsumerObservableAutoCommit[K, V](cfg, consumer)

  /** Builds a [[KafkaConsumerObservable]] instance.
    *
    * @param cfg    is the [[KafkaConsumerConfig]] needed for initializing the
    *               consumer; also make sure to see `monix/kafka/default.conf` for
    *               the default values being used.
    * @param topics is the list of Kafka topics to subscribe to.
    */
  def apply[K, V](cfg: KafkaConsumerConfig, topics: List[String])(implicit
    K: Deserializer[K],
    V: Deserializer[V]): KafkaConsumerObservable[K, V, ConsumerRecord[K, V]] = {

    val consumer = createConsumer[K, V](cfg, topics)
    apply(cfg, consumer)
  }

  /** Builds a [[KafkaConsumerObservable]] instance.
    *
    * @param cfg         is the [[KafkaConsumerConfig]] needed for initializing the
    *                    consumer; also make sure to see `monix/kafka/default.conf` for
    *                    the default values being used.
    * @param topicsRegex is the pattern of Kafka topics to subscribe to.
    */
  def apply[K, V](cfg: KafkaConsumerConfig, topicsRegex: Regex)(implicit
    K: Deserializer[K],
    V: Deserializer[V]): KafkaConsumerObservable[K, V, ConsumerRecord[K, V]] = {

    val consumer = createConsumer[K, V](cfg, topicsRegex)
    apply(cfg, consumer)
  }

  /** Builds a [[KafkaConsumerObservable]] instance with ability to manual commit offsets
    * and forcibly disables auto commits in configuration.
    * Such instances emit [[CommittableMessage]] instead of Kafka's ConsumerRecord.
    *
    * Usage example:
    * {{{
    *   KafkaConsumerObservable.manualCommit[String,String](consumerCfg, List(topicName))
    *     .map(message => message.record.value() -> message.committableOffset)
    *     .mapEval { case (value, offset) => performBusinessLogic(value).map(_ => offset) }
    *     .bufferTimedAndCounted(1.second, 1000)
    *     .mapEval(offsets => CommittableOffsetBatch(offsets).commitSync())
    *     .subscribe()
    * }}}
    *
    * @param cfg      is the [[KafkaConsumerConfig]] needed for initializing the
    *                 consumer; also make sure to see `monix/kafka/default.conf` for
    *                 the default values being used. Auto commit will disabled and
    *                 observable commit order will turned to [[monix.kafka.config.ObservableCommitOrder.NoAck NoAck]] forcibly!
    * @param consumer is a factory for the
    *                 `org.apache.kafka.clients.consumer.KafkaConsumer`
    *                 instance to use for consuming from Kafka
    */
  def manualCommit[K, V](
    cfg: KafkaConsumerConfig,
    consumer: Task[Consumer[K, V]]): KafkaConsumerObservable[K, V, CommittableMessage[K, V]] = {

    val manualCommitConfig = cfg
      .copy(observableCommitOrder = ObservableCommitOrder.NoAck, enableAutoCommit = false)
      .withPollHeartBeatRate(cfg.pollHeartbeatRate)
    new KafkaConsumerObservableManualCommit[K, V](manualCommitConfig, consumer)
  }

  /** Builds a [[KafkaConsumerObservable]] instance with ability to manual commit offsets
    * and forcibly disables auto commits in configuration.
    * Such instances emit [[CommittableMessage]] instead of Kafka's ConsumerRecord.
    *
    * Usage example:
    * {{{
    *   KafkaConsumerObservable.manualCommit[String,String](consumerCfg, List(topicName))
    *     .map(message => message.record.value() -> message.committableOffset)
    *     .mapEval { case (value, offset) => performBusinessLogic(value).map(_ => offset) }
    *     .bufferTimedAndCounted(1.second, 1000)
    *     .mapEval(offsets => CommittableOffsetBatch(offsets).commitSync())
    *     .subscribe()
    * }}}
    *
    * @param cfg    is the [[KafkaConsumerConfig]] needed for initializing the
    *               consumer; also make sure to see `monix/kafka/default.conf` for
    *               the default values being used. Auto commit will disabled and
    *               observable commit order will turned to [[monix.kafka.config.ObservableCommitOrder.NoAck NoAck]] forcibly!
    * @param topics is the list of Kafka topics to subscribe to.
    */
  def manualCommit[K, V](cfg: KafkaConsumerConfig, topics: List[String])(implicit
    K: Deserializer[K],
    V: Deserializer[V]): KafkaConsumerObservable[K, V, CommittableMessage[K, V]] = {

    val consumer = createConsumer[K, V](cfg, topics)
    manualCommit(cfg, consumer)
  }

  /** Builds a [[KafkaConsumerObservable]] instance with ability to manual commit offsets
    * and forcibly disables auto commits in configuration.
    * Such instances emit [[CommittableMessage]] instead of Kafka's ConsumerRecord.
    *
    * Usage example:
    * {{{
    *   KafkaConsumerObservable.manualCommit[String,String](consumerCfg, List(topicName))
    *     .map(message => message.record.value() -> message.committableOffset)
    *     .mapEval { case (value, offset) => performBusinessLogic(value).map(_ => offset) }
    *     .bufferTimedAndCounted(1.second, 1000)
    *     .mapEval(offsets => CommittableOffsetBatch(offsets).commitSync())
    *     .subscribe()
    * }}}
    *
    * @param cfg         is the [[KafkaConsumerConfig]] needed for initializing the
    *                    consumer; also make sure to see `monix/kafka/default.conf` for
    *                    the default values being used. Auto commit will disabled and
    *                    observable commit order will turned to [[monix.kafka.config.ObservableCommitOrder.NoAck NoAck]] forcibly!
    * @param topicsRegex is the pattern of Kafka topics to subscribe to.
    */
  def manualCommit[K, V](cfg: KafkaConsumerConfig, topicsRegex: Regex)(implicit
    K: Deserializer[K],
    V: Deserializer[V]): KafkaConsumerObservable[K, V, CommittableMessage[K, V]] = {

    val consumer = createConsumer[K, V](cfg, topicsRegex)
    manualCommit(cfg, consumer)
  }

  /** Returns a `Task` for creating a consumer instance given list of topics. */
  def createConsumer[K, V](config: KafkaConsumerConfig, topics: List[String])(implicit
    K: Deserializer[K],
    V: Deserializer[V]): Task[Consumer[K, V]] = {

    Task.evalAsync {
      val configMap = config.toJavaMap
      blocking {
        val consumer = new KafkaConsumer[K, V](configMap, K.create(), V.create())
        consumer.subscribe(topics.asJava)
        consumer
      }
    }
  }

  /** Returns a `Task` for creating a consumer instance given topics regex. */
  def createConsumer[K, V](config: KafkaConsumerConfig, topicsRegex: Regex)(implicit
    K: Deserializer[K],
    V: Deserializer[V]): Task[Consumer[K, V]] = {
    Task.evalAsync {
      val configMap = config.toJavaMap
      blocking {
        val consumer = new KafkaConsumer[K, V](configMap, K.create(), V.create())
        consumer.subscribe(topicsRegex.pattern)
        consumer
      }
    }
  }
}
