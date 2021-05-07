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

import java.io.File
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import monix.kafka.config._

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

/** The Kafka Producer config.
  *
  * For the official documentation on the available configuration
  * options, see
  * [[https://kafka.apache.org/documentation.html#producerconfigs Producer Configs]]
  * on `kafka.apache.org`.
  *
  * @param bootstrapServers is the `bootstrap.servers` setting
  *        and represents the list of servers to connect to.
  *
  * @param acks is the `acks` setting and represents
  *        the number of acknowledgments the producer requires the leader to
  *        have received before considering a request complete.
  *        See [[monix.kafka.config.Acks Acks]].
  *
  * @param bufferMemoryInBytes is the `buffer.memory` setting and
  *        represents the total bytes of memory the producer
  *        can use to buffer records waiting to be sent to the server.
  *
  * @param compressionType is the `compression.type` setting and specifies
  *        what compression algorithm to apply to all the generated data
  *        by the producer. The default is none (no compression applied).
  *
  * @param retries is the `retries` setting. A value greater than zero will
  *        cause the client to resend any record whose send fails with
  *        a potentially transient error.
  *
  * @param sslKeyPassword is the `ssl.key.password` setting and represents
  *        the password of the private key in the key store file.
  *        This is optional for client.
  *
  * @param sslKeyStorePassword is the `ssl.keystore.password` setting,
  *        being the password of the private key in the key store file.
  *        This is optional for client.
  *
  * @param sslKeyStoreLocation is the `ssl.keystore.location` setting and
  *        represents the location of the key store file. This is optional
  *        for client and can be used for two-way authentication for client.
  *
  * @param sslTrustStoreLocation is the `ssl.truststore.location` setting
  *        and is the location of the trust store file.
  *
  * @param sslTrustStorePassword is the `ssl.truststore.password` setting
  *        and is the password for the trust store file.
  *
  * @param batchSizeInBytes is the `batch.size` setting.
  *        The producer will attempt to batch records together into fewer
  *        requests whenever multiple records are being sent to the
  *        same partition. This setting specifies the maximum number of
  *        records to batch together.
  *
  * @param clientId is the `client.id` setting,
  *        an id string to pass to the server when making requests.
  *        The purpose of this is to be able to track the source of
  *        requests beyond just ip/port by allowing a logical application
  *        name to be included in server-side request logging.
  *
  * @param connectionsMaxIdleTime is the `connections.max.idle.ms` setting
  *        and specifies how much time to wait before closing idle connections.
  *
  * @param lingerTime is the `linger.ms` setting
  *        and specifies to buffer records for more efficient batching,
  *        up to the maximum batch size or for the maximum `lingerTime`.
  *        If zero, then no buffering will happen, but if different
  *        from zero, then records will be delayed in absence of load.
  *
  * @param maxBlockTime is the `max.block.ms` setting.
  *        The configuration controls how long `KafkaProducer.send()` and
  *        `KafkaProducer.partitionsFor()` will block. These methods can be
  *        blocked either because the buffer is full or metadata unavailable.
  *
  * @param maxRequestSizeInBytes is the `max.request.size` setting
  *        and represents the maximum size of a request in bytes.
  *        This is also effectively a cap on the maximum record size.
  *
  * @param maxInFlightRequestsPerConnection is the `max.in.flight.requests.per.connection` setting
  *        and represents the maximum number of unacknowledged request the client will send
  *        on a single connection before blocking.
  *        If this setting is set to be greater than 1 and there are failed sends,
  *        there is a risk of message re-ordering due to retries (if enabled).
  *
  * @param partitionerClass is the `partitioner.class` setting
  *        and represents a class that implements the
  *        `org.apache.kafka.clients.producer.Partitioner` interface.
  *
  * @param receiveBufferInBytes is the `receive.buffer.bytes` setting
  *        being the size of the TCP receive buffer (SO_RCVBUF) to use
  *        when reading data.
  *
  * @param requestTimeout is the `request.timeout.ms` setting,
  *        a configuration the controls the maximum amount of time
  *        the client will wait for the response of a request.
  *
  * @param saslKerberosServiceName is the `sasl.kerberos.service.name` setting,
  *        being the Kerberos principal name that Kafka runs as.
  *
  * @param saslMechanism is the `sasl.mechanism` setting, being the SASL
  *        mechanism used for client connections. This may be any mechanism
  *        for which a security provider is available.
  *
  * @param securityProtocol is the `security.protocol` setting,
  *        being the protocol used to communicate with brokers.
  *
  * @param sendBufferInBytes is the `send.buffer.bytes` setting,
  *        being the size of the TCP send buffer (SO_SNDBUF) to use
  *        when sending data.
  *
  * @param sslEnabledProtocols is the `ssl.enabled.protocols` setting,
  *        being the list of protocols enabled for SSL connections.
  *
  * @param sslKeystoreType is the `ssl.keystore.type` setting,
  *        being the file format of the key store file.
  *
  * @param sslProtocol is the `ssl.protocol` setting,
  *        being the SSL protocol used to generate the SSLContext.
  *        Default setting is TLS, which is fine for most cases.
  *        Allowed values in recent JVMs are TLS, TLSv1.1 and TLSv1.2. SSL,
  *        SSLv2 and SSLv3 may be supported in older JVMs, but their usage
  *        is discouraged due to known security vulnerabilities.
  *
  * @param sslProvider is the `ssl.provider` setting,
  *        being the name of the security provider used for SSL connections.
  *        Default value is the default security provider of the JVM.
  *
  * @param sslTruststoreType is the `ssl.truststore.type` setting, being
  *        the file format of the trust store file.
  *
  * @param reconnectBackoffTime is the `reconnect.backoff.ms` setting.
  *        The amount of time to wait before attempting to reconnect to a
  *        given host. This avoids repeatedly connecting to a host in a
  *        tight loop. This backoff applies to all requests sent by the
  *        consumer to the broker.
  *
  * @param retryBackoffTime is the `retry.backoff.ms` setting.
  *        The amount of time to wait before attempting to retry a failed
  *        request to a given topic partition. This avoids repeatedly
  *        sending requests in a tight loop under some failure scenarios.
  *
  * @param metadataMaxAge is the `metadata.max.age.ms` setting.
  *        The period of time in milliseconds after which we force a
  *        refresh of metadata even if we haven't seen any partition
  *        leadership changes to proactively discover any new brokers
  *        or partitions.
  *
  * @param metricReporters is the `metric.reporters` setting.
  *         A list of classes to use as metrics reporters. Implementing the
  *         `MetricReporter` interface allows plugging in classes that will
  *         be notified of new metric creation. The JmxReporter is always
  *         included to register JMX statistics
  *
  * @param metricsNumSamples is the `metrics.num.samples` setting.
  *         The number of samples maintained to compute metrics.
  *
  * @param metricsSampleWindow is the `metrics.sample.window.ms` setting.
  *         The metrics system maintains a configurable number of samples over
  *         a fixed window size. This configuration controls the size of the
  *         window. For example we might maintain two samples each measured
  *         over a 30 second period. When a window expires we erase and
  *         overwrite the oldest window.
  *
  * @param monixSinkParallelism is the `monix.producer.sink.parallelism`
  *        setting indicating how many requests the [[KafkaProducerSink]]
  *        can execute in parallel.
  *
  * @param properties map of other properties that will be passed to
  *        the underlying kafka client. Any properties not explicitly handled
  *        by this object can be set via the map, but in case of a duplicate
  *        a value set on the case class will overwrite value set via properties.
  */
final case class KafkaProducerConfig(
  bootstrapServers: List[String],
  acks: Acks,
  bufferMemoryInBytes: Int,
  compressionType: CompressionType,
  retries: Int,
  sslKeyPassword: Option[String],
  sslKeyStorePassword: Option[String],
  sslKeyStoreLocation: Option[String],
  sslTrustStoreLocation: Option[String],
  sslTrustStorePassword: Option[String],
  batchSizeInBytes: Int,
  clientId: String,
  connectionsMaxIdleTime: FiniteDuration,
  lingerTime: FiniteDuration,
  maxBlockTime: FiniteDuration,
  maxRequestSizeInBytes: Int,
  maxInFlightRequestsPerConnection: Int,
  partitionerClass: Option[PartitionerName],
  receiveBufferInBytes: Int,
  requestTimeout: FiniteDuration,
  saslKerberosServiceName: Option[String],
  saslMechanism: String,
  securityProtocol: SecurityProtocol,
  sendBufferInBytes: Int,
  sslEnabledProtocols: List[SSLProtocol],
  sslKeystoreType: String,
  sslProtocol: SSLProtocol,
  sslProvider: Option[String],
  sslTruststoreType: String,
  reconnectBackoffTime: FiniteDuration,
  retryBackoffTime: FiniteDuration,
  metadataMaxAge: FiniteDuration,
  metricReporters: List[String],
  metricsNumSamples: Int,
  metricsSampleWindow: FiniteDuration,
  monixSinkParallelism: Int,
  properties: Map[String, String]) {

  def toMap: Map[String, String] = properties ++ Map(
    "bootstrap.servers" -> bootstrapServers.mkString(","),
    "acks" -> acks.id,
    "buffer.memory" -> bufferMemoryInBytes.toString,
    "compression.type" -> compressionType.id,
    "retries" -> retries.toString,
    "ssl.key.password" -> sslKeyPassword.orNull,
    "ssl.keystore.password" -> sslKeyStorePassword.orNull,
    "ssl.keystore.location" -> sslKeyStoreLocation.orNull,
    "ssl.truststore.password" -> sslTrustStorePassword.orNull,
    "ssl.truststore.location" -> sslTrustStoreLocation.orNull,
    "batch.size" -> batchSizeInBytes.toString,
    "client.id" -> clientId,
    "connections.max.idle.ms" -> connectionsMaxIdleTime.toMillis.toString,
    "linger.ms" -> lingerTime.toMillis.toString,
    "max.block.ms" -> maxBlockTime.toMillis.toString,
    "max.request.size" -> maxRequestSizeInBytes.toString,
    "max.in.flight.requests.per.connection" -> maxInFlightRequestsPerConnection.toString,
    "partitioner.class" -> partitionerClass.map(_.className).orNull,
    "receive.buffer.bytes" -> receiveBufferInBytes.toString,
    "request.timeout.ms" -> requestTimeout.toMillis.toString,
    "sasl.kerberos.service.name" -> saslKerberosServiceName.orNull,
    "sasl.mechanism" -> saslMechanism,
    "security.protocol" -> securityProtocol.id,
    "send.buffer.bytes" -> sendBufferInBytes.toString,
    "ssl.enabled.protocols" -> sslEnabledProtocols.map(_.id).mkString(","),
    "ssl.keystore.type" -> sslKeystoreType,
    "ssl.protocol" -> sslProtocol.id,
    "ssl.provider" -> sslProvider.orNull,
    "ssl.truststore.type" -> sslTruststoreType,
    "reconnect.backoff.ms" -> reconnectBackoffTime.toMillis.toString,
    "retry.backoff.ms" -> retryBackoffTime.toMillis.toString,
    "metadata.max.age.ms" -> metadataMaxAge.toMillis.toString,
    "metric.reporters" -> metricReporters.mkString(","),
    "metrics.num.samples" -> metricsNumSamples.toString,
    "metrics.sample.window.ms" -> metricsSampleWindow.toMillis.toString
  )

  def toJavaMap: java.util.Map[String, Object] =
    toMap.filter(_._2 != null).map { case (a, b) => (a, b.asInstanceOf[AnyRef]) }.asJava

  def toProperties: Properties = {
    val props = new Properties()
    for ((k, v) <- toMap; if v != null) props.put(k, v)
    props
  }
}

object KafkaProducerConfig {
  private val defaultRootPath = "kafka"

  lazy private val defaultConf: Config =
    ConfigFactory.load("monix/kafka/default.conf").getConfig(defaultRootPath)

  /** Returns the default configuration, specified the `monix-kafka` project
    * in `monix/kafka/default.conf`.
    */
  lazy val default: KafkaProducerConfig =
    apply(defaultConf, includeDefaults = false)

  /** Loads the [[KafkaProducerConfig]] either from a file path or
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
  def load(): KafkaProducerConfig =
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

  /** Loads a [[KafkaProducerConfig]] from a project resource.
    *
    * @param resourceBaseName is the resource from where to load the config
    * @param rootPath is the config root path (e.g. `kafka`)
    * @param includeDefaults should be `true` in case you want to fallback
    *        to the default values provided by the `monix-kafka` library
    *        in `monix/kafka/default.conf`
    */
  def loadResource(
    resourceBaseName: String,
    rootPath: String = defaultRootPath,
    includeDefaults: Boolean = true): KafkaProducerConfig =
    apply(ConfigFactory.load(resourceBaseName).getConfig(rootPath), includeDefaults)

  /** Loads a [[KafkaProducerConfig]] from a specified file.
    *
    * @param file is the configuration path from where to load the config
    * @param rootPath is the config root path (e.g. `kafka`)
    * @param includeDefaults should be `true` in case you want to fallback
    *        to the default values provided by the `monix-kafka` library
    *        in `monix/kafka/default.conf`
    */
  def loadFile(file: File, rootPath: String = defaultRootPath, includeDefaults: Boolean = true): KafkaProducerConfig =
    apply(ConfigFactory.parseFile(file).resolve().getConfig(rootPath), includeDefaults)

  /** Loads the [[KafkaProducerConfig]] from a parsed
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
  def apply(source: Config, includeDefaults: Boolean = true): KafkaProducerConfig = {
    val config = if (!includeDefaults) source else source.withFallback(defaultConf)
    def getOptString(path: String): Option[String] =
      if (config.hasPath(path)) Option(config.getString(path))
      else None

    KafkaProducerConfig(
      bootstrapServers = config.getString("bootstrap.servers").trim.split("\\s*,\\s*").toList,
      acks = Acks(config.getString("acks")),
      bufferMemoryInBytes = config.getInt("buffer.memory"),
      compressionType = CompressionType(config.getString("compression.type")),
      retries = config.getInt("retries"),
      sslKeyPassword = getOptString("ssl.key.password"),
      sslKeyStorePassword = getOptString("ssl.keystore.password"),
      sslKeyStoreLocation = getOptString("ssl.keystore.location"),
      sslTrustStorePassword = getOptString("ssl.truststore.password"),
      sslTrustStoreLocation = getOptString("ssl.truststore.location"),
      batchSizeInBytes = config.getInt("batch.size"),
      clientId = config.getString("client.id"),
      connectionsMaxIdleTime = config.getInt("connections.max.idle.ms").millis,
      lingerTime = config.getInt("linger.ms").millis,
      maxBlockTime = config.getInt("max.block.ms").millis,
      maxRequestSizeInBytes = config.getInt("max.request.size"),
      maxInFlightRequestsPerConnection = config.getInt("max.in.flight.requests.per.connection"),
      partitionerClass = getOptString("partitioner.class").filter(_.nonEmpty).map(PartitionerName.apply),
      receiveBufferInBytes = config.getInt("receive.buffer.bytes"),
      requestTimeout = config.getInt("request.timeout.ms").millis,
      saslKerberosServiceName = getOptString("sasl.kerberos.service.name"),
      saslMechanism = config.getString("sasl.mechanism"),
      securityProtocol = SecurityProtocol(config.getString("security.protocol")),
      sendBufferInBytes = config.getInt("send.buffer.bytes"),
      sslEnabledProtocols = config.getString("ssl.enabled.protocols").split("\\s*,\\s*").map(SSLProtocol.apply).toList,
      sslKeystoreType = config.getString("ssl.keystore.type"),
      sslProtocol = SSLProtocol(config.getString("ssl.protocol")),
      sslProvider = getOptString("ssl.provider"),
      sslTruststoreType = config.getString("ssl.truststore.type"),
      reconnectBackoffTime = config.getInt("reconnect.backoff.ms").millis,
      retryBackoffTime = config.getInt("retry.backoff.ms").millis,
      metadataMaxAge = config.getInt("metadata.max.age.ms").millis,
      metricReporters = config.getString("metric.reporters").trim.split("\\s*,\\s*").toList,
      metricsNumSamples = config.getInt("metrics.num.samples"),
      metricsSampleWindow = config.getInt("metrics.sample.window.ms").millis,
      monixSinkParallelism = config.getInt("monix.producer.sink.parallelism"),
      properties = Map.empty
    )
  }
}
