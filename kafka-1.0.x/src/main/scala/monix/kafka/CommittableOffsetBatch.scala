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

/** Batch of Kafka offsets which can be committed together.
  * Can be built from offsets sequence by [[CommittableOffsetBatch#apply]] method.
  * You can also use [[CommittableOffsetBatch#empty]] method to create empty batch and
  * add offsets to it using [[updated]] method.
  *
  * WARNING: Order of the offsets is important. Only the last added offset
  * for topic and partition will be committed to Kafka.
  *
  * @param offsets is the offsets batch for a provided topics and partitions.
  *        Make sure that each of them was received from one [[KafkaConsumerObservable]].
  *
  * @param commitCallback is the set of callbacks for batched commit realized as closure
  *        in [[KafkaConsumerObservable]] context. This parameter is obtained from the last [[CommittableOffset]]
  *        added to batch.
  */
final class CommittableOffsetBatch private[kafka] (val offsets: Map[TopicPartition, Long], commitCallback: Commit) {

  /**
    * Synchronously commits [[offsets]] to Kafka
    * */
  def commitSync(): Task[Unit] = commitCallback.commitBatchSync(offsets)

  /**
    * Asynchronously commits [[offsets]] to Kafka
    * */
  def commitAsync(): Task[Unit] = commitCallback.commitBatchAsync(offsets)

  /**
    * Adds new [[CommittableOffset]] to batch. Added offset replaces previous one specified
    * for same topic and partition.
    * */
  def updated(committableOffset: CommittableOffset): CommittableOffsetBatch =
    new CommittableOffsetBatch(
      offsets.updated(committableOffset.topicPartition, committableOffset.offset),
      committableOffset.commitCallback
    )
}

object CommittableOffsetBatch {

  /**
    * Creates empty [[CommittableOffsetBatch]]. Can be used as neutral element in fold:
    * {{{
    *   offsets.foldLeft(CommittableOffsetBatch.empty)(_ updated _)
    * }}}
    * */
  val empty: CommittableOffsetBatch = new CommittableOffsetBatch(Map.empty, Commit.empty)

  /**
    * Builds [[CommittableOffsetBatch]] from offsets sequence. Be careful with
    * sequence order. If there is more than once offset for a topic and partition in the
    * sequence then the last one will remain.
    * */
  def apply(offsets: Seq[CommittableOffset]): CommittableOffsetBatch =
    if (offsets.nonEmpty) {
      val aggregatedOffsets = offsets.foldLeft(Map.empty[TopicPartition, Long]) { (acc, o) =>
        acc.updated(o.topicPartition, o.offset)
      }
      new CommittableOffsetBatch(aggregatedOffsets, offsets.head.commitCallback)
    } else {
      empty
    }

  /**
    * Builds [[CommittableOffsetBatch]] list from offsets sequence by merging the offsets
    * that have the same commit callback. This will help when the committable offsets are
    * from different consumers.
    * {{{
    *   CommittableOffsetBatch.mergeByCommitCallback(offsets)
    * }}}
    *
    * */
  def mergeByCommitCallback(committableOffsets: Seq[CommittableOffset]): List[CommittableOffsetBatch] = {
    if (committableOffsets.nonEmpty) {
      committableOffsets
        .groupBy(_.commitCallback)
        .mapValues(CommittableOffsetBatch(_))
        .values
        .toList
    } else {
      List.empty
    }
  }
}
