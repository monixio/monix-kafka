package monix.kafka.benchmarks

import monix.kafka.config.ObservableCommitOrder.BeforeAck
import monix.kafka.config.ObservableCommitType

import java.util.concurrent.TimeUnit
import monix.kafka.{KafkaConsumerObservable, KafkaProducerSink}
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.openjdk.jmh.annotations.{BenchmarkMode, Fork, Measurement, Mode, OutputTimeUnit, Scope, State, Threads, Warmup, _}
import scala.concurrent.duration._

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 1)
@Warmup(iterations = 1)
@Fork(1)
@Threads(3)
class ConsumerBenchmark extends MonixFixture {

  var totalRecords: Int = 1100
  val consumedRecords = 1000
  var maxPollRecords: Int = 1

  @Setup
  def setup(): Unit = {
    // preparing test data
    Observable
      .from(0 to totalRecords)
      .map(i => new ProducerRecord[Integer, Integer](monixTopic, i))
      .bufferTumbling(totalRecords)
      .consumeWith(KafkaProducerSink(producerConf.copy(monixSinkParallelism = 10), io))
      .runSyncUnsafe()
  }

  @Benchmark
  def monixAsyncAutoCommitHeartbeat15ms(): Unit = {
    val conf = consumerConf.value().copy(
      maxPollRecords = maxPollRecords,
      observableCommitType = ObservableCommitType.Async,
      observableCommitOrder = BeforeAck)
      .withPollHeartBeatRate(15.millis)

    KafkaConsumerObservable[Integer, Integer](conf, List(monixTopic))
      .take(consumedRecords)
      .headL
      .runSyncUnsafe()
  }

  @Benchmark
  def monixSyncAutoCommitHeartbeat15ms(): Unit = {
    val conf = consumerConf.value().copy(
      maxPollRecords = maxPollRecords,
      observableCommitType = ObservableCommitType.Sync,
      observableCommitOrder = BeforeAck)
      .withPollHeartBeatRate(15.millis)

    KafkaConsumerObservable[Integer, Integer](conf, List(monixTopic))
      .take(consumedRecords)
      .headL
      .runSyncUnsafe()
  }

  @Benchmark
  def monixManualCommitHeartbeat1ms(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(1.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(consumedRecords)
      .headL
      .runSyncUnsafe()
  }

  @Benchmark
  def monixManualCommitHeartbeat10ms(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(10.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(consumedRecords)
      .headL
      .runSyncUnsafe()
  }

  @Benchmark
  def monixManualCommitHeartbeat15ms(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(15.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(consumedRecords)
      .headL
      .runSyncUnsafe()
  }

  @Benchmark
  def monixManualCommitHeartbeat50ms(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(50.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(consumedRecords)
      .headL
      .runSyncUnsafe()
  }

  @Benchmark
  def monixManualCommitHeartbeat100ms(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(100.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(consumedRecords)
      .headL
      .runSyncUnsafe()
  }

  @Benchmark
  def monixManualCommitHeartbeat1000ms(): Unit = {
    val conf = consumerConf.value().copy(maxPollRecords = maxPollRecords)
      .withPollHeartBeatRate(1000.millis)

    KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .mapEvalF(_.committableOffset.commitAsync())
      .take(consumedRecords)
      .headL
      .runSyncUnsafe()
  }


}
