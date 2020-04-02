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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.OffsetCommitCallback

/**
  * Callback for batched commit realized as closure in [[KafkaConsumerObservable]] context.
  * */
trait Commit {
  def commitBatchSync(batch: Map[TopicPartition, Long]): Task[Unit]
  def commitBatchAsync(batch: Map[TopicPartition, Long]): Task[Unit]
}

private[kafka] object Commit {

  val empty: Commit = new Commit {
    override def commitBatchSync(batch: Map[TopicPartition, Long]): Task[Unit] = Task.unit
    override def commitBatchAsync(batch: Map[TopicPartition, Long]): Task[Unit] = Task.unit
  }
}
