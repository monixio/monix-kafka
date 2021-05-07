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
import org.apache.kafka.common.TopicPartition

/** Represents offset for specified topic and partition that can be
  * committed synchronously by [[commitSync]] method call or asynchronously by one of commitAsync methods.
  * To achieve good performance it is recommended to use batched commit with
  * [[CommittableOffsetBatch]] class.
  *
  * @param topicPartition is the topic and partition identifier
  *
  * @param offset is the offset to be committed
  *
  * @param commitCallback is the set of callbacks for batched commit realized as closures
  *        in [[KafkaConsumerObservable]] context.
  */
final class CommittableOffset private[kafka] (
  val topicPartition: TopicPartition,
  val offset: Long,
  private[kafka] val commitCallback: Commit) {

  /** Synchronously commits [[offset]] for the [[topicPartition]] to Kafka. It is recommended
    * to use batched commit with [[CommittableOffsetBatch]] class.
    */
  def commitSync(): Task[Unit] = commitCallback.commitBatchSync(Map(topicPartition -> offset))

  /** Asynchronously commits [[offset]] to Kafka. It is recommended
    * to use batched commit with [[CommittableOffsetBatch]] class.
    */
  def commitAsync(): Task[Unit] = commitCallback.commitBatchAsync(Map(topicPartition -> offset))
}

object CommittableOffset {

  private[kafka] def apply(topicPartition: TopicPartition, offset: Long, commitCallback: Commit): CommittableOffset =
    new CommittableOffset(topicPartition, offset, commitCallback)
}
