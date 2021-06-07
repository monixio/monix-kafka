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

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{KStream, Predicate, Printed, Produced}

import scala.collection.JavaConverters._
import scala.language.postfixOps

/** Scala wrapper for [[org.apache.kafka.streams.kstream.KStream]]
  *
  * @param underlyingStream the underlying [[org.apache.kafka.streams.kstream.KStream]] object
  * @tparam K key type
  * @tparam V value type
  */
final case class KafkaStream[K, V] private[streams](underlyingStream: KStream[K, V]) {

  def map[K1, V1](f: (K, V) => (K1, V1)): KafkaStream[K1, V1] =
    copy(
      underlyingStream = underlyingStream.map[K1, V1] { (key, value) =>
        val (newKey, newValue) = f(key, value)
        new KeyValue[K1, V1](newKey, newValue)
      }
    )

  def mapValues[V1](f: V => V1): KafkaStream[K, V1] =
    copy(underlyingStream = underlyingStream.mapValues[V1](f(_)))

  def subFlatMap[K1, V1](f: (K, V) => Iterable[(K1, V1)]): KafkaStream[K1, V1] =
    copy(
      underlyingStream = underlyingStream.flatMap[K1, V1] { (key: K, value: V) =>
        f(key, value).map { case (k, v) => new KeyValue[K1, V1](k, v) } asJava
      }
    )

  def filter(f: (K, V) => Boolean): KafkaStream[K, V] = {
    copy(
      underlyingStream = underlyingStream.filter(f(_, _))
    )
  }

  def filterNot(f: (K, V) => Boolean): KafkaStream[K, V] = {
    copy(
      underlyingStream = underlyingStream.filterNot(f(_, _))
    )
  }

  def branch(f: ((K, V) => Boolean)*): List[KStream[K, V]] = {
    underlyingStream.branch(
      f.map { fs =>
        new Predicate[K, V] {
          def test(key: K, value: V): Boolean = fs(key, value)
        }
      }:_*
    ).toList
  }

  def to(topic: String): KafkaStream[K, V] = {
    underlyingStream.to(topic)
    this
  }

  def producedTo(topic: String)(implicit keySerde: Serde[K], valueSerde: Serde[V]): KafkaStream[K, V] = {
    underlyingStream.to(topic, Produced.`with`(keySerde, valueSerde))
    this
  }

  def toSysOut(label: String): KafkaStream[K, V] = {
    underlyingStream.print(Printed.toSysOut[K, V].withLabel(label))
    this
  }

}

object KafkaStream {

  implicit val stringSerde: Serde[String] = Serdes.String
}
