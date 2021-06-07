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

import java.io.File
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import monix.kafka.streams.config.{ProcessingGuarantee, SecurityProtocol}

import scala.concurrent.duration._

/** The Kafka Streams Config
  *
  * @param applicationId
  * @param applicationServer
  * @param bootstrapServers
  * @param bufferedRecordsPerPartition
  * @param cacheMaxBytesBuffering
  * @param clientId
  * @param commitInterval
  * @param connectionsMaxIdle
  * @param defaultDeserializationExceptionHandler
  * @param defaultKeySerde
  * @param defaultTimestampExtractor
  * @param defaultValueSerde
  * @param metadataMaxAge
  * @param metricsNumSamples
  * @param metricsRecordLevel
  * @param metricReporters
  * @param metricsSampleWindow
  * @param numStandbyReplicas
  * @param numStreamThreads
  * @param partitionGrouper
  * @param poll
  * @param processingGuarantee
  * @param receiveBufferBytes
  * @param reconnectBackoff
  * @param reconnectBackoffMax
  * @param replicationFactor
  * @param requestTimeout
  * @param retryBackoff
  * @param rocksdbConfigSetter
  * @param securityProtocol
  * @param sendBufferBytes
  * @param stateCleanupDelay
  * @param stateDir
  * @param windowstoreChangelogAdditionalRetention
  */
final case class KafkaStreamsConfig(
  applicationId: String,
  applicationServer: String,
  bootstrapServers: List[String],
  bufferedRecordsPerPartition: Int,
  cacheMaxBytesBuffering: Long,
  clientId: String,
  commitInterval: FiniteDuration,
  connectionsMaxIdle: FiniteDuration,
  defaultDeserializationExceptionHandler: String,
  defaultKeySerde: String,
  defaultTimestampExtractor: String,
  defaultValueSerde: String,
  metadataMaxAge: FiniteDuration,
  metricsNumSamples: Int,
  metricsRecordLevel: String,
  metricReporters: List[String],
  metricsSampleWindow: FiniteDuration,
  numStandbyReplicas: Int,
  numStreamThreads: Int,
  partitionGrouper: String,
  poll: FiniteDuration,
  processingGuarantee: ProcessingGuarantee,
  receiveBufferBytes: Int,
  reconnectBackoff: FiniteDuration,
  reconnectBackoffMax: FiniteDuration,
  replicationFactor: Int,
  requestTimeout: FiniteDuration,
  retryBackoff: FiniteDuration,
  rocksdbConfigSetter: String,
  securityProtocol: SecurityProtocol,
  sendBufferBytes: Int,
  stateCleanupDelay: FiniteDuration,
  stateDir: String,
  windowstoreChangelogAdditionalRetention: FiniteDuration) {

  def toProperties: Properties = {
    val props = new Properties()
    for ((k,v) <- toMap; if v != null) props.put(k,v)
    props
  }

  def toMap: Map[String,String] = Map(
    "application.id" -> applicationId,
    "application.server" -> applicationServer,
    "bootstrap.servers" -> bootstrapServers.mkString(","),
    "buffered.records.per.partition" -> bufferedRecordsPerPartition.toString,
    "cache.max.bytes.buffering" -> cacheMaxBytesBuffering.toString,
    "client.id" -> clientId,
    "commit.interval.ms" -> commitInterval.toMillis.toString,
    "connections.max.idle.ms" -> connectionsMaxIdle.toMillis.toString,
    "default.deserialization.exception.handler" -> defaultDeserializationExceptionHandler,
    "default.key.serde" -> defaultKeySerde,
    "default.timestamp.extractor" -> defaultTimestampExtractor,
    "default.value.serde" -> defaultValueSerde,
    "metadata.max.age.ms" -> metadataMaxAge.toMillis.toString,
    "metrics.num.samples" -> metricsNumSamples.toString,
    "metrics.record.level" -> metricsRecordLevel,
    "metric.reporters" -> metricReporters.mkString(","),
    "metrics.sample.window.ms" -> metricsSampleWindow.toMillis.toString,
    "num.standby.replicas" -> numStandbyReplicas.toString,
    "num.stream.threads" -> numStreamThreads.toString,
    "partition.grouper" -> partitionGrouper.toString,
    "poll.ms" -> poll.toMillis.toString,
    "processing.guarantee" -> processingGuarantee.id,
    "receive.buffer.bytes" -> receiveBufferBytes.toString,
    "reconnect.backoff.ms" -> reconnectBackoff.toMillis.toString,
    "reconnect.backoff.max.ms" -> reconnectBackoffMax.toMillis.toString,
    "replication.factor" -> replicationFactor.toString,
    "request.timeout.ms" -> requestTimeout.toMillis.toString,
    "retry.backoff.ms" -> retryBackoff.toMillis.toString,
    "rocksdb.config.setter" -> rocksdbConfigSetter,
    "security.protocol" -> securityProtocol.id,
    "send.buffer.bytes" -> sendBufferBytes.toString,
    "state.cleanup.delay.ms" -> stateCleanupDelay.toMillis.toString,
    "state.dir" -> stateDir,
    "windowstore.changelog.additional.retention.ms" -> windowstoreChangelogAdditionalRetention.toMillis.toString
  )
}

object KafkaStreamsConfig {
  private val defaultRootPath = "kafka"

  lazy private val defaultConf: Config =
    ConfigFactory.load("monix/kafka/default.conf").getConfig(defaultRootPath)

  /** Returns the default configuration, specified the `monix-kafka` project
    * in `monix/kafka/default.conf`.
    */
  lazy val default: KafkaStreamsConfig =
    apply(defaultConf, includeDefaults = false)

  /** Loads the [[KafkaStreamsConfig]] either from a file path or
    * from a resource, if `config.file` or `config.resource` are
    * defined, or otherwise returns the default config.
    *
    * If you want to specify a `config.file`, you can configure the
    * Java process on execution like so:
    * {{{
    *   java -Dconfig.file=/path/to/application.conf
    * }}}
    *
    * Or if you want to specify a `config.resource` to be loaded
    * from the executable's distributed JAR or classpath:
    * {{{
    *   java -Dconfig.resource=com/company/mySpecial.conf
    * }}}
    *
    * In case neither of these are specified, then the configuration
    * loaded is the default one, from the `monix-kafka` project, specified
    * in `monix/kafka/default.conf`.
    */
  def load(): KafkaStreamsConfig =
    Option(System.getProperty("config.file")).map(f => new File(f)) match {
      case Some(file) if file.exists() =>
        loadFile(file)
      case None =>
        Option(System.getProperty("config.resource")) match {
          case Some(resource) =>
            loadResource(resource)
          case None =>
            default
        }
    }

  /** Loads a [[KafkaStreamsConfig]] from a project resource.
    *
    * @param resourceBaseName is the resource from where to load the config
    * @param rootPath is the config root path (e.g. `kafka`)
    * @param includeDefaults should be `true` in case you want to fallback
    *        to the default values provided by the `monix-kafka` library
    *        in `monix/kafka/default.conf`
    */
  def loadResource(resourceBaseName: String, rootPath: String = defaultRootPath, includeDefaults: Boolean = true): KafkaStreamsConfig =
    apply(ConfigFactory.load(resourceBaseName).getConfig(rootPath), includeDefaults)

  /** Loads a [[KafkaStreamsConfig]] from a specified file.
    *
    * @param file is the configuration path from where to load the config
    * @param rootPath is the config root path (e.g. `kafka`)
    * @param includeDefaults should be `true` in case you want to fallback
    *        to the default values provided by the `monix-kafka` library
    *        in `monix/kafka/default.conf`
    */
  def loadFile(file: File, rootPath: String = defaultRootPath, includeDefaults: Boolean = true): KafkaStreamsConfig =
    apply(ConfigFactory.parseFile(file).resolve().getConfig(rootPath), includeDefaults)

  /** Loads the [[KafkaStreamsConfig]] from a parsed
    * `com.typesafe.config.Config` reference.
    *
    * NOTE that this method doesn't assume any path prefix for loading the
    * configuration settings, so it does NOT assume a root path like `kafka`.
    * In case case you need that, you can always do:
    *
    * {{{
    *   KafkaProducerConfig(globalConfig.getConfig("kafka"))
    * }}}
    *
    * @param source is the typesafe `Config` object to read from
    * @param includeDefaults should be `true` in case you want to fallback
    *        to the default values provided by the `monix-kafka` library
    *        in `monix/kafka/default.conf`
    */
  def apply(source: Config, includeDefaults: Boolean = true): KafkaStreamsConfig = {
    val config = if (!includeDefaults) source else source.withFallback(defaultConf)
    def getOptString(path: String): Option[String] =
      if (config.hasPath(path)) Option(config.getString(path))
      else None

    KafkaStreamsConfig(
      applicationId = config.getString("application.id"),
      applicationServer = config.getString("application.server"),
      bootstrapServers = config.getString("bootstrap.servers").trim.split("\\s*,\\s*").toList,
      bufferedRecordsPerPartition = config.getInt("buffered.records.per.partition"),
      cacheMaxBytesBuffering = config.getLong("cache.max.bytes.buffering"),
      clientId = config.getString("client.id"),
      commitInterval = config.getInt("commit.interval.ms").millis,
      connectionsMaxIdle = config.getInt("connections.max.idle.ms").millis,
      defaultDeserializationExceptionHandler = config.getString("default.deserialization.exception.handler"),
      defaultKeySerde = config.getString("default.key.serde"),
      defaultTimestampExtractor = config.getString("default.timestamp.extractor"),
      defaultValueSerde = config.getString("default.value.serde"),
      metadataMaxAge = config.getInt("metadata.max.age.ms").millis,
      metricsNumSamples = config.getInt("metrics.num.samples"),
      metricsRecordLevel = config.getString("metrics.record.level"),
      metricReporters = config.getString("metric.reporters").trim.split("\\s*,\\s*").toList,
      metricsSampleWindow = config.getInt("metrics.sample.window.ms").millis,
      numStandbyReplicas = config.getInt("num.standby.replicas"),
      numStreamThreads = config.getInt("num.stream.threads"),
      partitionGrouper = config.getString("partition.grouper"),
      poll = config.getInt("poll.ms").millis,
      processingGuarantee = ProcessingGuarantee(config.getString("processing.guarantee")),
      receiveBufferBytes = config.getInt("receive.buffer.bytes"),
      reconnectBackoff = config.getInt("reconnect.backoff.ms").millis,
      reconnectBackoffMax = config.getInt("reconnect.backoff.max.ms").millis,
      replicationFactor = config.getInt("replication.factor"),
      requestTimeout = config.getInt("request.timeout.ms").millis,
      retryBackoff = config.getInt("retry.backoff.ms").millis,
      rocksdbConfigSetter = getOptString("rocksdb.config.setter").filter(_.nonEmpty).orNull,
      securityProtocol = SecurityProtocol(config.getString("security.protocol")),
      sendBufferBytes = config.getInt("send.buffer.bytes"),
      stateCleanupDelay = config.getInt("state.cleanup.delay.ms").millis,
      stateDir = config.getString("state.dir"),
      windowstoreChangelogAdditionalRetention = config.getInt("windowstore.changelog.additional.retention.ms").millis
    )
  }
}
