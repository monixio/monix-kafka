/*
 * Copyright (c) 2014-2019 by its authors. Some rights reserved.
 * See the project homepage at: https://github.com/monix/monix-kafka
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
import monix.execution.Scheduler.Implicits.global
import monix.kafka.config.AutoOffsetReset
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class PollHeartbeatTest extends FunSuite with KafkaTestKit with ScalaFutures {

  val topicName = "monix-kafka-tests"

  val producerCfg = KafkaProducerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:6001"),
    clientId = "monix-kafka-1-0-producer-test"
  )

  val consumerCfg = KafkaConsumerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:6001"),
    groupId = "kafka-tests",
    clientId = "monix-kafka-1-0-consumer-test",
    autoOffsetReset = AutoOffsetReset.Earliest
  )

  test("slow downstream with small poll heartbeat and manual async commit keeps the consumer assignment") {
    withRunningKafka {

      val count = 250
      val topicName = "monix-kafka-manual-commit-tests"
      val delay = 200.millis
      val pollHeartbeat = 2.millis
      val fastPollHeartbeatConfig =
        consumerCfg.copy(maxPollInterval = 200.millis, maxPollRecords = 1).withPollHeartBeatRate(pollHeartbeat)

      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable.manualCommit[String, String](fastPollHeartbeatConfig, List(topicName))

      val pushT = Observable
        .fromIterable(1 to count)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .mapEval(producer.send)
        .lastL

      val listT = consumer
        .executeOn(io)
        .doOnNextF { committableMessage =>
          Task.sleep(delay) *>
            CommittableOffsetBatch(Seq(committableMessage.committableOffset)).commitAsync().executeAsync
        }
        .take(count)
        .toListL

      val (committableMessages, _) = Task.parZip2(listT.executeAsync, pushT.executeAsync).runSyncUnsafe(100.seconds)
      val CommittableMessage(lastRecord, lastCommittableOffset) = committableMessages.last
      assert(pollHeartbeat * 10 < delay)
      assert((1 to count).sum === committableMessages.map(_.record.value().toInt).sum)
      assert(lastRecord.value().toInt === count)
      assert(count === lastCommittableOffset.offset)
      assert(new TopicPartition(topicName, 0) === lastCommittableOffset.topicPartition)
    }
  }

  test("auto committable consumer with slow processing doesn't cause rebalancing") {
    withRunningKafka {
      val count = 10000

      val consumerConfig = consumerCfg.copy(
        maxPollInterval = 200.millis,
        heartbeatInterval = 10.millis,
        maxPollRecords = 1
      )

      val producer = KafkaProducerSink[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable[String, String](consumerConfig, List(topicName)).executeOn(io)

      val pushT = Observable
        .range(0, count)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .bufferIntrospective(1024)
        .consumeWith(producer)

      val listT = consumer
        .take(count)
        .map(_.value())
        .bufferTumbling(count / 4)
        .mapEval(s => Task.sleep(2.second) >> Task.delay(s))
        .flatMap(Observable.fromIterable)
        .toListL

      val (result, _) = Task.parZip2(listT.executeAsync, pushT.delayExecution(1.second).executeAsync).runSyncUnsafe()
      assert(result.map(_.toInt).sum === (0 until count).sum)
    }
  }

  test("slow committable downstream with small poll heartbeat does not cause rebalancing") {
    withRunningKafka {
      val totalRecords = 1000
      val topicName = "monix-kafka-manual-commit-tests"
      val downstreamLatency = 40.millis
      val pollHeartbeat = 1.millis
      val maxPollInterval = 10.millis
      val maxPollRecords = 1
      val fastPollHeartbeatConfig =
        consumerCfg
          .copy(maxPollInterval = 200.millis, maxPollRecords = maxPollRecords)
          .withPollHeartBeatRate(pollHeartbeat)

      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable.manualCommit[String, String](fastPollHeartbeatConfig, List(topicName))

      val pushT = Observable
        .fromIterable(1 to totalRecords)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .mapEval(producer.send)
        .lastL

      val listT = consumer
        .executeOn(io)
        .mapEvalF { committableMessage =>
          val manualCommit = Task
            .defer(committableMessage.committableOffset.commitAsync())
            .as(committableMessage)
          Task.sleep(downstreamLatency) *> manualCommit
        }
        .take(totalRecords)
        .toListL

      val (committableMessages, _) =
        Task.parZip2(listT.executeAsync, pushT.delayExecution(100.millis).executeAsync).runSyncUnsafe()
      val CommittableMessage(lastRecord, lastCommittableOffset) = committableMessages.last
      assert(pollHeartbeat * 10 < downstreamLatency)
      assert(pollHeartbeat < maxPollInterval)
      assert(maxPollInterval < downstreamLatency)
      assert((1 to totalRecords).sum === committableMessages.map(_.record.value().toInt).sum)
      assert(lastRecord.value().toInt === totalRecords)
      assert(totalRecords === lastCommittableOffset.offset)
      assert(new TopicPartition(topicName, 0) === lastCommittableOffset.topicPartition)
    }
  }

  //unhappy scenario
  test("slow committable downstream with small `maxPollInterval` and high `pollHeartBeat` causes consumer rebalance") {
    withRunningKafka {
      val totalRecords = 200
      val topicName = "monix-kafka-manual-commit-tests"
      val downstreamLatency = 2.seconds
      val pollHeartbeat = 15.seconds
      val maxPollInterval = 100.millis
      val fastPollHeartbeatConfig =
        consumerCfg.copy(maxPollInterval = maxPollInterval, maxPollRecords = 1).withPollHeartBeatRate(pollHeartbeat)

      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable.manualCommit[String, String](fastPollHeartbeatConfig, List(topicName))

      val pushT = Observable
        .fromIterable(1 to totalRecords)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .mapEval(producer.send)
        .lastL

      val listT = consumer
        .executeOn(io)
        .mapEvalF { committableMessage =>
          val manualCommit = Task
            .defer(committableMessage.committableOffset.commitAsync())
            .as(committableMessage)
          Task.sleep(downstreamLatency) *> manualCommit
        }
        .take(totalRecords)
        .toListL

      assert(pollHeartbeat > downstreamLatency)
      assert(maxPollInterval < downstreamLatency)
      assert(fastPollHeartbeatConfig.pollHeartbeatRate === pollHeartbeat)

      val f = Task.parZip2(listT.executeAsync, pushT.executeAsync).map(_._1).delayResult(50.seconds).runToFuture

      /* checks that value never returns,
       * which is correct in this scenario since if the `maxPollInterval`
       * is higher than `pollHeartBeat` and `downstreamLatency`
       * on the other hand, it would be ideal to receive the following error message from kafka
       * "the group has already rebalanced and assigned the partitions to another member"
       * as it happens from kafka-client 1.1.0, see tests from kafka1x.
       */
      assert(f.value === None)
    }
  }

  test("super slow committable downstream causes consumer rebalance") {
    withRunningKafka {
      val totalRecords = 3
      val topicName = "monix-kafka-manual-commit-tests"
      val downstreamLatency = 55.seconds
      val pollHeartbeat = 5.seconds
      val maxPollInterval = 4.seconds
      // the downstreamLatency is higher than the `maxPollInterval`
      // but smaller than `pollHeartBeat`, kafka will trigger rebalance
      // and the consumer will be kicked out of the consumer group.
      val fastPollHeartbeatConfig =
        consumerCfg.copy(maxPollInterval = maxPollInterval, maxPollRecords = 1).withPollHeartBeatRate(pollHeartbeat)

      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable.manualCommit[String, String](fastPollHeartbeatConfig, List(topicName))

      val pushT = Observable
        .fromIterable(1 to totalRecords)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .mapEval(producer.send)
        .lastL

      val listT = consumer
        .executeOn(io)
        .doOnNextF { committableMessage =>
          val manualCommit = Task.defer(committableMessage.committableOffset.commitAsync())
          Task.sleep(downstreamLatency) *> manualCommit
        }
        .take(totalRecords)
        .toListL

      assert(pollHeartbeat * 10 < downstreamLatency)
      assert(maxPollInterval * 10 < downstreamLatency)
      assert(fastPollHeartbeatConfig.pollHeartbeatRate === pollHeartbeat)

      implicit val patienceConfig: PatienceConfig = PatienceConfig(30.seconds, 100.milliseconds)

      val f = Task.parZip2(listT.executeAsync, pushT.executeAsync).map(_._1).delayResult(50.seconds).runToFuture

      /* checks that value never returns,
       * which is correct in this scenario since if the `maxPollInterval`
       * is higher than `pollHeartBeat` and `downstreamLatency`
       * on the other hand, it would be ideal to receive the following error message from kafka
       * "the group has already rebalanced and assigned the partitions to another member"
       * as it happens from kafka-client 1.1.0, see tests from kafka1x.
       */
      assert(f.value === None)
    }
  }

}
