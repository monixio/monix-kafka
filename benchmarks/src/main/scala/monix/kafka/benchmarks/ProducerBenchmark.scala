package monix.kafka.benchmarks

import org.openjdk.jmh.annotations.{BenchmarkMode, Fork, Measurement, Mode, OutputTimeUnit, Scope, State, Threads, Warmup}

import java.util.concurrent.TimeUnit
import monix.eval.Task
import monix.kafka.{KafkaProducer, KafkaProducerSink}
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.openjdk.jmh.annotations._

import scala.concurrent.duration._
import scala.concurrent.Await

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 3)
@Warmup(iterations = 1)
@Fork(1)
@Threads(1)
class ProducerBenchmark extends MonixFixture {

  var size: Int = 1000
   val producer = KafkaProducer[Integer, Integer](producerConf, io)

   @Benchmark
    def monix_single_producer(): Unit = {
     val f = Task.traverse((0 until size).toList)(i => producer.send(topic = monixTopic, i)).runToFuture(io)
     Await.ready(f,  Duration.Inf)
    }

  @Benchmark
  def monix_sink_producer(): Unit = {
    val f = Observable
      .from(1 to size)
      .map(i => new ProducerRecord[Integer, Integer](monixTopic, i))
      .bufferTumbling(50)
      .consumeWith(KafkaProducerSink(producerConf.copy(monixSinkParallelism = 10), io))
      .runToFuture(io)
    Await.ready(f, Duration.Inf)
  }

}
