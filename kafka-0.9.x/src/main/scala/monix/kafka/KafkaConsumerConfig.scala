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

/** Configuration for Kafka Consumer.
  *
  * For the official documentation on the available configuration
  * options, see
  * [[https://kafka.apache.org/documentation.html#consumerconfigs Consumer Configs]]
  * on `kafka.apache.org`.
  *
  * @param bootstrapServers is the `bootstrap.servers` setting,
  *        a list of host/port pairs to use for establishing
  *        the initial connection to the Kafka cluster.
  *
  * @param fetchMinBytes is the `fetch.min.bytes` setting,
  *        the minimum amount of data the server should return
  *        for a fetch request.
  *
  * @param groupId is the `group.id` setting, a unique string
  *        that identifies the consumer group this consumer
  *        belongs to.
  *
  * @param heartbeatInterval is the `heartbeat.interval.ms` setting,
  *        the expected time between heartbeats to the consumer coordinator
  *        when using Kafka's group management facilities.
  *
  * @param maxPartitionFetchBytes is the `max.partition.fetch.bytes`
  *        setting, the maximum amount of data per-partition the
  *        server will return.
  *
  * @param sessionTimeout is the `session.timeout.ms` setting,
  *        the timeout used to detect failures when using Kafka's
  *        group management facilities.
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
  * @param autoOffsetReset is the `auto.offset.reset` setting,
  *        specifying what to do when there is no initial offset in
  *        Kafka or if the current offset does not exist any more
  *        on the server (e.g. because that data has been deleted).
  *
  * @param connectionsMaxIdleTime is the `connections.max.idle.ms` setting
  *        and specifies how much time to wait before closing idle connections.
  *
  * @param enableAutoCommit is the `enable.auto.commit` setting.
  *        If true the consumer's offset will be periodically committed
  *        in the background.
  *
  * @param excludeInternalTopics is the `exclude.internal.topics` setting.
  *        Whether records from internal topics (such as offsets) should be
  *        exposed to the consumer. If set to true the only way to receive
  *        records from an internal topic is subscribing to it.
  *
  * @param maxPollRecords is the `max.poll.records` setting, the
  *        maximum number of records returned in a single call to poll().
  *
  * @param receiveBufferInBytes is the `receive.buffer.bytes` setting,
  *        the size of the TCP receive buffer (SO_RCVBUF) to use
  *        when reading data.
  *
  * @param requestTimeout is the `request.timeout.ms` setting,
  *        The configuration controls the maximum amount of time
  *        the client will wait for the response of a request.
  *        If the response is not received before the timeout elapses
  *        the client will resend the request if necessary or fail the
  *        request if retries are exhausted.
  *
  * @param saslKerberosServiceName is the `sasl.kerberos.service.name` setting,
  *        being the Kerberos principal name that Kafka runs as.
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
  * @param checkCRCs is the `check.crcs` setting, specifying to
  *        automatically check the CRC32 of the records consumed.
  *        This ensures no on-the-wire or on-disk corruption to the
  *        messages occurred. This check adds some overhead, so it may
  *        be disabled in cases seeking extreme performance.
  *
  * @param clientId is the `client.id` setting,
  *        an id string to pass to the server when making requests.
  *        The purpose of this is to be able to track the source of
  *        requests beyond just ip/port by allowing a logical application
  *        name to be included in server-side request logging.
  *
  * @param fetchMaxWaitTime is the `fetch.max.wait.ms` setting,
  *        the maximum amount of time the server will block before
  *        answering the fetch request if there isn't sufficient data to
  *        immediately satisfy the requirement given by fetch.min.bytes.
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
  * @param observableCommitType is the `monix.observable.commit.type` setting.
  *        Represents the type of commit to make when the [[enableAutoCommit]]
  *        setting is set to `false`, in which case the observable has to
  *        commit on every batch.
  *
  * @param observableCommitOrder is the `monix.observable.commit.order` setting.
  *        Specifies when the commit should happen, like before we receive the
  *        acknowledgement from downstream, or afterwards.
  *
  * @param properties map of other properties that will be passed to
  *        the underlying kafka client. Any properties not explicitly handled
  *        by this object can be set via the map, but in case of a duplicate
  *        a value set on the case class will overwrite value set via properties.
  */
final case class KafkaConsumerConfig(
  bootstrapServers: List[String],
  fetchMinBytes: Int,
  groupId: String,
  heartbeatInterval: FiniteDuration,
  maxPartitionFetchBytes: Int,
  sessionTimeout: FiniteDuration,
  sslKeyPassword: Option[String],
  sslKeyStorePassword: Option[String],
  sslKeyStoreLocation: Option[String],
  sslTrustStoreLocation: Option[String],
  sslTrustStorePassword: Option[String],
  autoOffsetReset: AutoOffsetReset,
  connectionsMaxIdleTime: FiniteDuration,
  enableAutoCommit: Boolean,
  excludeInternalTopics: Boolean,
  maxPollRecords: Int,
  receiveBufferInBytes: Int,
  requestTimeout: FiniteDuration,
  saslKerberosServiceName: Option[String],
  securityProtocol: SecurityProtocol,
  sendBufferInBytes: Int,
  sslEnabledProtocols: List[SSLProtocol],
  sslKeystoreType: String,
  sslProtocol: SSLProtocol,
  sslProvider: Option[String],
  sslTruststoreType: String,
  checkCRCs: Boolean,
  clientId: String,
  fetchMaxWaitTime: FiniteDuration,
  metadataMaxAge: FiniteDuration,
  metricReporters: List[String],
  metricsNumSamples: Int,
  metricsSampleWindow: FiniteDuration,
  reconnectBackoffTime: FiniteDuration,
  retryBackoffTime: FiniteDuration,
  observableCommitType: ObservableCommitType,
  observableCommitOrder: ObservableCommitOrder,
  observableSeekOnStart: ObservableSeekOnStart,
  properties: Map[String, String]) {

  def toMap: Map[String, String] = properties ++ Map(
    "bootstrap.servers" -> bootstrapServers.mkString(","),
    "fetch.min.bytes" -> fetchMinBytes.toString,
    "group.id" -> groupId,
    "heartbeat.interval.ms" -> heartbeatInterval.toMillis.toString,
    "max.partition.fetch.bytes" -> maxPartitionFetchBytes.toString,
    "session.timeout.ms" -> sessionTimeout.toMillis.toString,
    "ssl.key.password" -> sslKeyPassword.orNull,
    "ssl.keystore.password" -> sslKeyStorePassword.orNull,
    "ssl.keystore.location" -> sslKeyStoreLocation.orNull,
    "ssl.truststore.password" -> sslTrustStorePassword.orNull,
    "ssl.truststore.location" -> sslTrustStoreLocation.orNull,
    "auto.offset.reset" -> autoOffsetReset.id,
    "connections.max.idle.ms" -> connectionsMaxIdleTime.toMillis.toString,
    "enable.auto.commit" -> enableAutoCommit.toString,
    "exclude.internal.topics" -> excludeInternalTopics.toString,
    "max.poll.records" -> maxPollRecords.toString,
    "receive.buffer.bytes" -> receiveBufferInBytes.toString,
    "request.timeout.ms" -> requestTimeout.toMillis.toString,
    "sasl.kerberos.service.name" -> saslKerberosServiceName.orNull,
    "security.protocol" -> securityProtocol.id,
    "send.buffer.bytes" -> sendBufferInBytes.toString,
    "ssl.enabled.protocols" -> sslEnabledProtocols.map(_.id).mkString(","),
    "ssl.keystore.type" -> sslKeystoreType,
    "ssl.protocol" -> sslProtocol.id,
    "ssl.provider" -> sslProvider.orNull,
    "ssl.truststore.type" -> sslTruststoreType,
    "check.crcs" -> checkCRCs.toString,
    "client.id" -> clientId,
    "fetch.max.wait.ms" -> fetchMaxWaitTime.toMillis.toString,
    "metadata.max.age.ms" -> metadataMaxAge.toMillis.toString,
    "metric.reporters" -> metricReporters.mkString(","),
    "metrics.num.samples" -> metricsNumSamples.toString,
    "metrics.sample.window.ms" -> metricsSampleWindow.toMillis.toString,
    "reconnect.backoff.ms" -> reconnectBackoffTime.toMillis.toString,
    "retry.backoff.ms" -> retryBackoffTime.toMillis.toString
  )

  def toJavaMap: java.util.Map[String, Object] =
    toMap.filter(_._2 != null).map { case (a, b) => (a, b.asInstanceOf[AnyRef]) }.asJava

  def toProperties: Properties = {
    val props = new Properties()
    for ((k, v) <- toMap; if v != null) props.put(k, v)
    props
  }
}

object KafkaConsumerConfig {
  private val defaultRootPath = "kafka"

  lazy private val defaultConf: Config =
    ConfigFactory.load("monix/kafka/default.conf").getConfig(defaultRootPath)

  /** Returns the default configuration, specified the `monix-kafka` project
    * in `monix/kafka/default.conf`.
    */
  lazy val default: KafkaConsumerConfig =
    apply(defaultConf, includeDefaults = false)

  /** Loads the [[KafkaConsumerConfig]] either from a file path or
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
  def load(): KafkaConsumerConfig =
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

  /** Loads a [[KafkaConsumerConfig]] from a project resource.
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
    includeDefaults: Boolean = true): KafkaConsumerConfig =
    apply(ConfigFactory.load(resourceBaseName).getConfig(rootPath), includeDefaults)

  /** Loads a [[KafkaConsumerConfig]] from a specified file.
    *
    * @param file is the configuration path from where to load the config
    * @param rootPath is the config root path (e.g. `kafka`)
    * @param includeDefaults should be `true` in case you want to fallback
    *        to the default values provided by the `monix-kafka` library
    *        in `monix/kafka/default.conf`
    */
  def loadFile(file: File, rootPath: String = defaultRootPath, includeDefaults: Boolean = true): KafkaConsumerConfig =
    apply(ConfigFactory.parseFile(file).resolve().getConfig(rootPath), includeDefaults)

  /** Loads the [[KafkaConsumerConfig]] from a parsed
    * `com.typesafe.config.Config` reference.
    *
    * NOTE that this method doesn't assume any path prefix for loading the
    * configuration settings, so it does NOT assume a root path like `kafka`.
    * In case case you need that, you can always do:
    *
    * {{{
    *   KafkaConsumerConfig(globalConfig.getConfig("kafka"))
    * }}}
    *
    * @param source is the typesafe `Config` object to read from
    * @param includeDefaults should be `true` in case you want to fallback
    *        to the default values provided by the `monix-kafka` library
    *        in `monix/kafka/default.conf`
    */
  def apply(source: Config, includeDefaults: Boolean = true): KafkaConsumerConfig = {
    val config = if (!includeDefaults) source else source.withFallback(defaultConf)
    def getOptString(path: String): Option[String] =
      if (config.hasPath(path)) Option(config.getString(path))
      else None

    KafkaConsumerConfig(
      bootstrapServers = config.getString("bootstrap.servers").trim.split("\\s*,\\s*").toList,
      fetchMinBytes = config.getInt("fetch.min.bytes"),
      groupId = config.getString("group.id"),
      heartbeatInterval = config.getInt("heartbeat.interval.ms").millis,
      maxPartitionFetchBytes = config.getInt("max.partition.fetch.bytes"),
      sessionTimeout = config.getInt("session.timeout.ms").millis,
      sslKeyPassword = getOptString("ssl.key.password"),
      sslKeyStorePassword = getOptString("ssl.keystore.password"),
      sslKeyStoreLocation = getOptString("ssl.keystore.location"),
      sslTrustStorePassword = getOptString("ssl.truststore.password"),
      sslTrustStoreLocation = getOptString("ssl.truststore.location"),
      autoOffsetReset = AutoOffsetReset(config.getString("auto.offset.reset")),
      connectionsMaxIdleTime = config.getInt("connections.max.idle.ms").millis,
      enableAutoCommit = config.getBoolean("enable.auto.commit"),
      excludeInternalTopics = config.getBoolean("exclude.internal.topics"),
      maxPollRecords = config.getInt("max.poll.records"),
      receiveBufferInBytes = config.getInt("receive.buffer.bytes"),
      requestTimeout = config.getInt("request.timeout.ms").millis,
      saslKerberosServiceName = getOptString("sasl.kerberos.service.name"),
      securityProtocol = SecurityProtocol(config.getString("security.protocol")),
      sendBufferInBytes = config.getInt("send.buffer.bytes"),
      sslEnabledProtocols = config.getString("ssl.enabled.protocols").split("\\s*,\\s*").map(SSLProtocol.apply).toList,
      sslKeystoreType = config.getString("ssl.keystore.type"),
      sslProtocol = SSLProtocol(config.getString("ssl.protocol")),
      sslProvider = getOptString("ssl.provider"),
      sslTruststoreType = config.getString("ssl.truststore.type"),
      checkCRCs = config.getBoolean("check.crcs"),
      clientId = config.getString("client.id"),
      fetchMaxWaitTime = config.getInt("fetch.max.wait.ms").millis,
      metadataMaxAge = config.getInt("metadata.max.age.ms").millis,
      metricReporters = config.getString("metric.reporters").trim.split("\\s*,\\s*").toList,
      metricsNumSamples = config.getInt("metrics.num.samples"),
      metricsSampleWindow = config.getInt("metrics.sample.window.ms").millis,
      reconnectBackoffTime = config.getInt("reconnect.backoff.ms").millis,
      retryBackoffTime = config.getInt("retry.backoff.ms").millis,
      observableCommitType = ObservableCommitType(config.getString("monix.observable.commit.type")),
      observableCommitOrder = ObservableCommitOrder(config.getString("monix.observable.commit.order")),
      observableSeekOnStart = ObservableSeekOnStart(config.getString("monix.observable.seek.onStart")),
      properties = Map.empty
    )
  }
}
