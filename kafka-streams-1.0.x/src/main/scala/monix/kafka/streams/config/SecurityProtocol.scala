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

/** The `security.protocol` setting for the Kafka Producer.
  *
  * Represents the protocol used to communicate with brokers.
  *
  * Valid values are:
  *
  *  - [[SecurityProtocol.PLAINTEXT]]
  *  - [[SecurityProtocol.SSL]]
  *  - [[SecurityProtocol.SASL_PLAINTEXT]]
  *  - [[SecurityProtocol.SASL_SSL]]
  */
sealed trait SecurityProtocol extends Serializable {
  def id: String
}

object SecurityProtocol {
  @throws(classOf[BadValue])
  def apply(id: String): SecurityProtocol =
    id match {
      case PLAINTEXT.id => PLAINTEXT
      case SSL.id => SSL
      case SASL_PLAINTEXT.id => SASL_PLAINTEXT
      case SASL_SSL.id => SASL_SSL
      case _ =>
        throw new BadValue("kafka.security.protocol", s"Invalid value: $id")
    }

  case object PLAINTEXT extends SecurityProtocol {
    val id = "PLAINTEXT"
  }

  case object SSL extends SecurityProtocol {
    val id = "SSL"
  }

  case object SASL_PLAINTEXT extends SecurityProtocol {
    val id = "SASL_PLAINTEXT"
  }

  case object SASL_SSL extends SecurityProtocol {
    val id = "SASL_SSL"
  }
}
