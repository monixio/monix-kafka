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

package monix.kafka.streams.config

import com.typesafe.config.ConfigException.BadValue

/** The `processing.guarantee` setting for the Kafka Streams.
  *
  * The processing guarantee that should be used.
  *
  * Valid values are:
  *
  *  - [[ProcessingGuarantee.AtLeastOnce]]
  *  - [[ProcessingGuarantee.ExactlyOnce]]
  */
sealed trait ProcessingGuarantee extends Serializable {
  def id: String
}

object ProcessingGuarantee {
  @throws(classOf[BadValue])
  def apply(id: String): ProcessingGuarantee =
    id match {
      case AtLeastOnce.id => AtLeastOnce
      case ExactlyOnce.id => ExactlyOnce
      case _ =>
        throw new BadValue("kafka.security.protocol", s"Invalid value: $id")
    }

  case object AtLeastOnce extends ProcessingGuarantee {
    val id = "at_least_once"
  }

  case object ExactlyOnce extends ProcessingGuarantee {
    val id = "exactly_once"
  }
}

