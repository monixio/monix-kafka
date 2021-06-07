package monix.kafka

import java.util

import monix.eval.Task
import monix.kafka.config.AutoOffsetReset
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Serializer => KafkaSerializer}
import org.apache.kafka.common.serialization.{Deserializer => KafkaDeserializer}

import scala.concurrent.duration._
import scala.concurrent.Await
import monix.execution.Scheduler.Implicits.global
import monix.execution.exceptions.DummyException
import org.scalatest.funsuite.AnyFunSuite

class SerializationTest extends AnyFunSuite with KafkaTestKit {

  val producerCfg = KafkaProducerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:6001"),
    clientId = "monix-kafka-1-0-serialization-test"
  )

  val consumerCfg = KafkaConsumerConfig.default.copy(
    bootstrapServers = List("127.0.0.1:6001"),
    groupId = "kafka-tests",
    clientId = "monix-kafka-1-0-serialization-test",
    autoOffsetReset = AutoOffsetReset.Earliest
  )

  test("serialization/deserialization using kafka.common.serialization") {
    withRunningKafka {
      val topicName = "monix-kafka-serialization-tests"
      val count = 10000

      implicit val serializer: KafkaSerializer[A] = new ASerializer
      implicit val deserializer: KafkaDeserializer[A] = new ADeserializer

      val producer = KafkaProducerSink[String, A](producerCfg, io)
      val consumer = KafkaConsumerObservable[String, A](consumerCfg, List(topicName)).executeOn(io).take(count)

      val pushT = Observable
        .range(0, count)
        .map(msg => new ProducerRecord(topicName, "obs", A(msg.toString)))
        .bufferIntrospective(1024)
        .consumeWith(producer)

      val listT = consumer
        .map(_.value())
        .toListL

      val (result, _) = Await.result(Task.parZip2(listT, pushT).runToFuture, 60.seconds)
      assert(result.map(_.value.toInt).sum === (0 until count).sum)
    }
  }

  test("allow to fail the stream if serialization throws") {
    withRunningKafka {
      val topicName = "monix-kafka-serialization-failing-tests"
      val dummy = DummyException("boom")

      implicit val serializer: KafkaSerializer[A] = new AFailingSerializer

      val producer = KafkaProducerSink[String, A](producerCfg, io, (_: Throwable) => Task.raiseError(dummy))

      val pushT = Observable
        .evalOnce(new ProducerRecord(topicName, "obs", A(1.toString)))
        .bufferIntrospective(1024)
        .consumeWith(producer)

      assertThrows[DummyException] {
        Await.result(pushT.runToFuture, 60.seconds)
      }
    }
  }

  test("allow to recover from serialization errors") {
    withRunningKafka {
      val topicName = "monix-kafka-serialization-continuing-tests"
      val count = 100

      implicit val serializer: KafkaSerializer[A] = new AHalfFailingSerializer
      implicit val deserializer: KafkaDeserializer[A] = new ADeserializer

      val producer = KafkaProducerSink[String, A](producerCfg, io)
      val consumer = KafkaConsumerObservable[String, A](consumerCfg, List(topicName)).executeOn(io).take(count / 2)

      val pushT = Observable
        .range(0, count)
        .map(msg => new ProducerRecord(topicName, "obs", A(msg.toString)))
        .bufferIntrospective(1024)
        .consumeWith(producer)

      val listT = consumer
        .map(_.value())
        .toListL

      val (result, _) = Await.result(Task.parZip2(listT, pushT).runToFuture, 60.seconds)
      assert(result.map(_.value.toInt).sum === (0 until count).filter(_ % 2 == 0).sum)
    }
  }

}

case class A(value: String)

class ASerializer extends KafkaSerializer[A] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def serialize(topic: String, data: A): Array[Byte] =
    if (data == null) null else data.value.getBytes

  override def close(): Unit = ()
}

class ADeserializer extends KafkaDeserializer[A] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): A = if (data == null) null else A(new String(data))

  override def close(): Unit = ()
}

class AFailingSerializer extends ASerializer {
  override def serialize(topic: String, data: A): Array[Byte] = throw new RuntimeException("fail")
}

class AHalfFailingSerializer extends ASerializer {

  override def serialize(topic: String, data: A): Array[Byte] = {
    if (data.value.toInt % 2 == 0) super.serialize(topic, data)
    else throw new RuntimeException("fail")
  }
}
