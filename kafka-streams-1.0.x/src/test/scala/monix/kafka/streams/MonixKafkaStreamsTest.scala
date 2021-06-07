/*
 * Copyright (c) 2014-2016 by its authors. Some rights reserved.
 * See the project homepage at: https://github.com/monixio/monix-kafka
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

package monix.kafka.streams

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.scalatest.FunSuite
import scala.concurrent.Await
import scala.concurrent.duration._

class MonixKafkaStreamsTest extends FunSuite with EmbeddedKafka {

  implicit val stringSerde: Serde[String] = Serdes.String

  val config = KafkaStreamsConfig.default.copy(
    bootstrapServers = List("127.0.0.1:6001"),
    applicationId = "monix-kafka-streams-1-0-streams-test"
  )

  val inTopic = "in-topic"
  val outTopic = "out-topic"

  test("publish one message") {

    withRunningKafka {
      createCustomTopic(inTopic)
      createCustomTopic(outTopic)
      publishStringMessageToKafka(inTopic, "hello")

      val topo = StreamTopology()

      topo
        .streamWith[String, String](inTopic)
        .mapValues(_.toUpperCase)
        .producedTo(outTopic)
        .toSysOut("Test App")

      val streams = KafkaStreams(config, topo)

      val flow = for {
        _ <- Task.now(println("App Started"))
        _ <- streams.start
        _ <- Task.sleep(5.seconds)
        _ <- Task.now(println("Shutting down APP now"))
        _ <- streams.close
      } yield ()

      Await.result(flow.runAsync, 10.seconds)

      val message: String = consumeFirstStringMessageFrom(outTopic)
      assert(message == "HELLO")

    }



  }
}