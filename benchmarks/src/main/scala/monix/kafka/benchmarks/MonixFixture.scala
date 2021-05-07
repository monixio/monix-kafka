package monix.kafka.benchmarks

import monix.eval.Coeval
import monix.execution.Scheduler
import monix.kafka.config.{AutoOffsetReset => MonixAutoOffsetReset}
import monix.kafka.{KafkaConsumerConfig, KafkaProducerConfig}

trait MonixFixture extends BaseFixture{

  implicit lazy val io = Scheduler.io("monix-kafka-benchmark")

  val producerConf = KafkaProducerConfig.default.copy(
    bootstrapServers = List(brokerUrl),
    clientId = monixTopic,
  )

  // we set a different group id every time so then the consumer will always
  // read from the beginning
  val consumerConf = Coeval(KafkaConsumerConfig.default.copy(
    bootstrapServers = List(brokerUrl),
    clientId = monixTopic,
    groupId = randomId.value(),
    enableAutoCommit = false,
    autoOffsetReset = MonixAutoOffsetReset.Earliest
  ))

}
