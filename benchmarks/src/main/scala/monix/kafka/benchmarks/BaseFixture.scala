package monix.kafka.benchmarks

import monix.eval.Coeval

import scala.util.Random

trait BaseFixture {
  protected val brokerUrl = "127.0.0.1:9092"
  protected val randomId: Coeval[String] = Coeval(Random.alphanumeric.filter(_.isLetter).take(20).mkString)
  protected val monixTopic = "monix_topic"
}
