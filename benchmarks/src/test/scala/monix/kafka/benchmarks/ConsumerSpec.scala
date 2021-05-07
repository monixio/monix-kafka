package monix.kafka.benchmarks

import monix.execution.Scheduler.Implicits.global
import monix.kafka.{KafkaConsumerObservable, KafkaProducer, KafkaProducerSink}
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class ConsumerSpec extends FlatSpec with MonixFixture with Matchers with BeforeAndAfterAll {


  override def beforeAll(): Unit = {
    super.beforeAll()
    Observable
      .from(1 to 1000)
      .map(i => new ProducerRecord[Integer, Integer](monixTopic, i))
      .bufferTumbling(100)
      .consumeWith(KafkaProducerSink(producerConf.copy(monixSinkParallelism = 10), io))
      .runSyncUnsafe()
  }

  s"Monix ${monixTopic}" should "exist" in {
    val producer = KafkaProducer[String, String](producerConf, global)
    val t = producer.send(topic = monixTopic, "test")

    t.runSyncUnsafe().isDefined shouldBe true
  }

  it should "consume with autocommit" in {
    val conf = consumerConf.value()

    val f2 = KafkaConsumerObservable[Integer, Integer](conf, List(monixTopic)).take(1000).toListL.runToFuture(io)

    val elements = Await.result(f2, 10.seconds)
    elements.size shouldBe 1000
  }

  it should "consume with manual commit" in {
    val conf = consumerConf.value()

    val f2 = KafkaConsumerObservable.manualCommit[Integer, Integer](conf, List(monixTopic))
      .take(1000).toListL.runToFuture(io)

    val elements = Await.result(f2, 10.seconds)
    elements.size shouldBe 1000
  }

}
