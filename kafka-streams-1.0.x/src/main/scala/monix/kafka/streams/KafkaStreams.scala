/*
 * Copyright (c) 2014-2018 by The Monix Project Developers.
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
import org.apache.kafka.streams.{StreamsConfig, KafkaStreams => ApacheKafkaStreams}

/** The wrapper for [[org.apache.kafka.streams.KafkaStreams]]
  *
  * @param underlying the underlying [[org.apache.kafka.streams.KafkaStreams]] object
  */
final class KafkaStreams private[streams](underlying: ApacheKafkaStreams) {

  def start: Task[Unit] =
    Task.eval(underlying.start())

  def close: Task[Unit] =
    Task.eval(underlying.close())

}

object KafkaStreams {

  def apply[K, V](config: KafkaStreamsConfig, topology: StreamTopology) = {
    new KafkaStreams(
      new ApacheKafkaStreams(topology.build, new StreamsConfig(config.toProperties))
    )
  }
}
