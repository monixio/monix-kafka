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

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

/**
  * Class for managing the stream builder and topology
  */
final class StreamTopology private {

  private val builder: StreamsBuilder = new StreamsBuilder

  def stream[K, V](topic: String): KafkaStream[K, V] =
    KafkaStream(builder.stream[K, V](topic))

  def streamWith[K, V](topic: String)(implicit keySerde: Serde[K], valueSerde: Serde[V]): KafkaStream[K, V] =
    KafkaStream(builder.stream[K, V](topic, Consumed.`with`(keySerde, valueSerde)))

  private[streams] def build: Topology =
    builder.build()
}

object StreamTopology {

  def apply(): StreamTopology =
    new StreamTopology

}
