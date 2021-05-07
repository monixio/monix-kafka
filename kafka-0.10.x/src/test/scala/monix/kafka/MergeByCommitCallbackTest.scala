package monix.kafka

import monix.eval.Task
import monix.kafka.config.AutoOffsetReset
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._
import scala.concurrent.Await
import monix.execution.Scheduler.Implicits.global
import org.apache.kafka.common.TopicPartition
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class MergeByCommitCallbackTest extends FunSuite with KafkaTestKit with ScalaCheckDrivenPropertyChecks with Matchers {

  val commitCallbacks: List[Commit] = List.fill(4)(new Commit {
    override def commitBatchSync(batch: Map[TopicPartition, Long]): Task[Unit] = Task.unit
    override def commitBatchAsync(batch: Map[TopicPartition, Long]): Task[Unit] = Task.unit
  })

  val committableOffsetsGen: Gen[CommittableOffset] = for {
    partition <- Gen.posNum[Int]
    offset <- Gen.posNum[Long]
    commit <- Gen.oneOf(commitCallbacks)
  } yield CommittableOffset(new TopicPartition("topic", partition), offset, commit)

  test("merge by commit callback works") {
    forAll(Gen.nonEmptyListOf(committableOffsetsGen)) { offsets =>
      val partitions = offsets.map(_.topicPartition)
      val received: List[CommittableOffsetBatch] = CommittableOffsetBatch.mergeByCommitCallback(offsets)

      received.foreach { batch => partitions should contain allElementsOf batch.offsets.keys }

      received.size should be <= 4
    }
  }

  test("merge by commit callback for multiple consumers") {
    withRunningKafka {
      val count = 10000
      val topicName = "monix-kafka-merge-by-commit"

      val producerCfg = KafkaProducerConfig.default.copy(
        bootstrapServers = List("127.0.0.1:6001"),
        clientId = "monix-kafka-1-0-producer-test"
      )

      val producer = KafkaProducerSink[String, String](producerCfg, io)

      val pushT = Observable
        .range(0, count)
        .map(msg => new ProducerRecord(topicName, "obs", msg.toString))
        .bufferIntrospective(1024)
        .consumeWith(producer)

      val listT = Observable
        .range(0, 4)
        .mergeMap(i => createConsumer(i.toInt, topicName).take(500))
        .bufferTumbling(2000)
        .map(CommittableOffsetBatch.mergeByCommitCallback)
        .map { offsetBatches => assert(offsetBatches.length == 4) }
        .completedL

      Await.result(Task.parZip2(listT, pushT).runToFuture, 60.seconds)
    }
  }

  private def createConsumer(i: Int, topicName: String): Observable[CommittableOffset] = {
    val cfg = KafkaConsumerConfig.default.copy(
      bootstrapServers = List("127.0.0.1:6001"),
      groupId = s"kafka-tests-$i",
      autoOffsetReset = AutoOffsetReset.Earliest
    )

    KafkaConsumerObservable
      .manualCommit[String, String](cfg, List(topicName))
      .executeOn(io)
      .map(_.committableOffset)
  }
}
