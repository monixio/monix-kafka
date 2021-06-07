package monix.kafka

import org.scalatest.funsuite.AnyFunSuite

class ConfigTest extends AnyFunSuite {
  test("overwrite properties with values from producer config") {
    val config =
      KafkaProducerConfig.default
        .copy(bootstrapServers = List("localhost:9092"), properties = Map("bootstrap.servers" -> "127.0.0.1:9092"))

    assert(
      config.toProperties.getProperty("bootstrap.servers") == "localhost:9092"
    )
  }

  test("overwrite properties with values from consumer config") {
    val config =
      KafkaConsumerConfig.default
        .copy(bootstrapServers = List("localhost:9092"), properties = Map("bootstrap.servers" -> "127.0.0.1:9092"))

    assert(
      config.toProperties.getProperty("bootstrap.servers") == "localhost:9092"
    )
  }

  test("convert to Java map from producer config and filter null values") {
    val config = KafkaProducerConfig.default.toJavaMap

    assert(!config.containsValue(null))
  }

  test("convert to Java map from consumer config and filter null values") {
    val config = KafkaConsumerConfig.default.toJavaMap

    assert(!config.containsValue(null))
  }
}
