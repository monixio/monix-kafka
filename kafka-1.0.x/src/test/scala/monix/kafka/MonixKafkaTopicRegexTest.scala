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
import org.scalatest.FunSuite

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class MonixKafkaTopicRegexTest extends FunSuite with KafkaTestKit {
  val topicsRegex = "monix-kafka-tests-.*".r
  val topicMatchingRegex = "monix-kafka-tests-anything"

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

  test("publish one message when subscribed to topics regex") {
    withRunningKafka {

      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumerTask = KafkaConsumerObservable.createConsumer[String, String](consumerCfg, topicsRegex).executeOn(io)
      val consumer = Await.result(consumerTask.runToFuture, 60.seconds)

      try {
        // Publishing one message
        val send = producer.send(topicMatchingRegex, "my-message")
        Await.result(send.runToFuture, 30.seconds)

        val records = consumer.poll(10.seconds.toMillis).asScala.map(_.value()).toList
        assert(records === List("my-message"))
      } finally {
        Await.result(producer.close().runToFuture, Duration.Inf)
        consumer.close()
      }
    }
  }

  test("listen for one message when subscribed to topics regex") {
    withRunningKafka {
      val producer = KafkaProducer[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable[String, String](consumerCfg, topicsRegex).executeOn(io)
      try {
        // Publishing one message
        val send = producer.send(topicMatchingRegex, "test-message")
        Await.result(send.runToFuture, 30.seconds)

        val first = consumer.take(1).map(_.value()).firstL
        val result = Await.result(first.runToFuture, 30.seconds)
        assert(result === "test-message")
      } finally {
        Await.result(producer.close().runToFuture, Duration.Inf)
      }
    }
  }

  test("full producer/consumer test when subscribed to topics regex") {
    withRunningKafka {
      val count = 10000

      val producer = KafkaProducerSink[String, String](producerCfg, io)
      val consumer = KafkaConsumerObservable[String, String](consumerCfg, topicsRegex).executeOn(io).take(count)

      val pushT = Observable
        .range(0, count)
        .map(msg => new ProducerRecord(topicMatchingRegex, "obs", msg.toString))
        .bufferIntrospective(1024)
        .consumeWith(producer)

      val listT = consumer
        .map(_.value())
        .toListL

      val (result, _) = Await.result(Task.parZip2(listT, pushT).runToFuture, 60.seconds)
      assert(result.map(_.toInt).sum === (0 until count).sum)
    }
  }
}
