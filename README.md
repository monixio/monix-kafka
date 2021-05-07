# Monix-Kafka

[![Build Status](https://travis-ci.org/monix/monix-kafka.svg?branch=master)](https://travis-ci.org/monix/monix-kafka)
[![Maven Central](https://img.shields.io/maven-central/v/io.monix/monix-kafka-1x_2.12.svg)](https://search.maven.org/search?q=g:io.monix%20AND%20a:monix-kafka-1x_2.12)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-brightgreen.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/monix/monix?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Monix integration with Kafka

Work in progress!

## Table of Contents
1. [Getting Started with Kafka 1.0.x or above](#getting-started-with-kafka-10x-or-above)
2. [Getting Started with Kafka 0.11.x](#getting-started-with-kafka-011x)
3. [Getting Started with Kafka 0.10.x](#getting-started-with-kafka-010x)
4. [Getting Started with Kafka 0.9.x](#getting-started-with-kafka-09x)
5. [Getting Started with Kafka 0.8.x (no longer supported)](#getting-started-with-kafka-08x)
6. [Usage](#usage)
7. [How can I contribute to Monix-Kafka?](#how-can-i-contribute-to-monix-kafka?)
8. [Maintainers](#maintainers)
9. [License](#license)

## Getting Started with Kafka 1.0.x or above

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-1x" % "1.0.0-RC6"
```

For `kafka` versions higher than `1.0.x` also add a dependency override:

```scala
dependencyOverrides += "org.apache.kafka" % "kafka-clients" % "2.1.0"
```

Or in case you're interested in running the tests of this project, it
now supports embedded kafka for integration testing. You can simply run:

```bash
sbt kafka1x/test
```

## Getting Started with Kafka 0.11.x

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-11" % "1.0.0-RC6"
```

Or in case you're interested in running the tests of this project, it
now supports embedded kafka for integration testing. You can simply run:

```bash
sbt kafka11/test
```

## Getting Started with Kafka 0.10.x

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-10" % "1.0.0-RC6"
```

Or in case you're interested in running the tests of this project, it
now supports embedded kafka for integration testing. You can simply run:

```bash
sbt kafka10/test
```

## Getting Started with Kafka 0.9.x

Please note that `EmbeddedKafka` is not supported for Kafka `0.9.x`

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-9" % "1.0.0-RC6"
```

Or in case you're interested in running the tests of this project,
first download the Kafka server, version `0.9.x` from their
[download page](https://kafka.apache.org/downloads.html) (note that
`0.10.x` or higher do not work with `0.9`), then as the
[quick start](https://kafka.apache.org/090/documentation.html#quickstart)
section says, open a terminal window and first start Zookeeper:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Then start Kafka:

```bash
bin/kafka-server-start.sh config/server.properties
```

Create the topic we need for our tests:

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 1 \
  --topic monix-kafka-tests
```

And run the tests:

```bash
sbt kafka9/test
```

## Getting Started with Kafka 0.8.x

Please note that support for Kafka `0.8.x` is dropped and the last available version with this dependency is `0.14`.

In SBT:

```scala
libraryDependencies += "io.monix" %% "monix-kafka-8" % "0.14"
```

Or in case you're interested in running the tests of this project,
first download the Kafka server, version `0.8.x` from their
[download page](https://kafka.apache.org/downloads.html) (note that
`0.9.x` or higher do not work with `0.8`), then as the
[quick start](https://kafka.apache.org/082/documentation.html#quickstart)
section says, open a terminal window and first start Zookeeper:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Then start Kafka:

```bash
bin/kafka-server-start.sh config/server.properties
```

Create the topics we need for our tests:

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 1 \
  --topic monix-kafka-tests
bin/kafka-topics.sh --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 1 \
  --topic monix-kafka-manual-commit-tests
```

And run the tests:

```bash
sbt kafka8/test
```

## Usage

### Producer

```scala
import monix.kafka._
import monix.execution.Scheduler

implicit val scheduler: Scheduler = monix.execution.Scheduler.global

// Init
val producerCfg = KafkaProducerConfig.default.copy(
  bootstrapServers = List("127.0.0.1:9092")
)

val producer = KafkaProducer[String,String](producerCfg, scheduler)

// For sending one message
val recordMetadataF = producer.send("my-topic", "my-message").runToFuture

// For closing the producer connection
val closeF = producer.close().runToFuture
```

Calling `producer.send` returns a [Task](https://monix.io/docs/3x/eval/task.html) of `Option[RecordMetadata]` which can then be run and transformed into a `Future`.

If the `Task` completes with `None` it means that `producer.send` method was called after the producer was closed and that the message wasn't successfully acknowledged by the Kafka broker. In case of the failure of the underlying Kafka client the producer will bubble up the exception and fail the `Task`.  All successfully delivered messages will complete with `Some[RecordMetadata]`.

For pushing an entire `Observable` to Apache Kafka:

```scala
import monix.kafka._
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord

implicit val scheduler: Scheduler = monix.execution.Scheduler.global

// Initializing the producer
val producerCfg = KafkaProducerConfig.default.copy(
  bootstrapServers = List("127.0.0.1:9092")
)

val producer = KafkaProducerSink[String,String](producerCfg, scheduler)

// Lets pretend we have this observable of records
val observable: Observable[ProducerRecord[String,String]] = ???

observable
  // on overflow, start dropping incoming events
  .whileBusyDrop
  // buffers into batches if the consumer is busy, up to a max size
  .bufferIntrospective(1024)
  // consume everything by pushing into Apache Kafka
  .consumeWith(producer)
  // ready, set, go!
  .runToFuture
```

### Consumer

There are several ways for consuming from Apache Kafka (Version 0.11.x and above):

Consumer which commits offsets itself:
```scala
import monix.kafka._

val consumerCfg = KafkaConsumerConfig.default.copy(
  bootstrapServers = List("127.0.0.1:9092"),
  groupId = "kafka-tests"
  // you can use this settings for At Most Once semantics:
  // observableCommitOrder = ObservableCommitOrder.BeforeAck
)

val observable =
  KafkaConsumerObservable[String,String](consumerCfg, List("my-topic"))
    .take(10000)
    .map(_.value())
```

Consumer which allows you to commit offsets manually:
```scala
import monix.kafka._

val consumerCfg = KafkaConsumerConfig.default.copy(
  bootstrapServers = List("127.0.0.1:9092"),
  groupId = "kafka-tests"
)

val observable =
  KafkaConsumerObservable.manualCommit[String,String](consumerCfg, List("my-topic"))
    .map(message => message.record.value() -> message.committableOffset)
    .mapEval { case (value, offset) => performBusinessLogic(value).map(_ => offset) }
    .bufferTimedAndCounted(1.second, 1000)
    .mapEval(offsets => CommittableOffsetBatch(offsets).commitSync())
```

Enjoy!

### Internal poll heartbeat

Starting from Kafka _0.10.1.0_, there is `max.poll.interval.ms` setting that defines the maximum delay between
invocations of poll(), if it is not called in that interval, then the consumer is considered failed and  the group will rebalance in order 
    to reassign the partitions to another member.

This was an [issue](https://github.com/monix/monix-kafka/issues/101) in `monix-kafka`, 
since poll is not called until all previous consumed ones were processed, so that slow downstream subscribers
 were in risk of being kicked off the consumer group indefinitely.

It is resolved in `1.0.0-RC8` by introducing an internal poll heartbeat interval
that runs in the background keeping the consumer alive. 

## How can I contribute to Monix-Kafka?

We welcome contributions to all projects in the Monix organization and would love
for you to help build Monix-Kafka. See our [contributor guide](./CONTRIBUTING.md) for
more information about how you can get involed.

## Maintainers

The current maintainers (people who can merge pull requests) are:

- Alexandru Nedelcu ([alexandru](https://github.com/alexandru))
- Piotr Gawryś ([Avasil](https://github.com/Avasil))
- Leandro Bolivar ([leandrob13](https://github.com/leandrob13))

## License

All code in this repository is licensed under the Apache License,
Version 2.0.  See [LICENSE.txt](./LICENSE).
