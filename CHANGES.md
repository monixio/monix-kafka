
## Version 1.0.0-RC7 (January 23, 2021)

Depends on Monix 3.3.0

Main changes:

- PR #248: Enable '-Xfatal-warnings' flag (internal)
- PR #245: Use Kafka Producer Consumer interfaces

## Version 1.0.0-RC6 (April 18, 2020)

Depends on Monix 3.x

Main changes:

- PR #133: Configure serializers in kafka producer after creating
- PR #150: Allow customization of error handling in ProducerSink
- PR #179: Add seekToBeginning option and reuse instances of kafka (de)serializers
- PR #180: Add client.rack option

Thanks to all the contributors!

## Version 1.0.0-RC5 (September 15, 2019)

Depends on Monix 3.0.0

Main changes:

- PR #89: Added fetch.max.bytes setting
- PR #95: Improve Producer performance
- PR #100: Add method `mergeByCommitCallback`
- PR #115: Allow to use Kafka Serializers

Thanks to all the contributors!

## Version 1.0.0-RC4 (June 16, 2019)

Depends on Monix `3.0.0-RC3`.

## Version 1.0.0-RC3 (May 1, 2019)

Changes:
- [PR #64](https://github.com/monix/monix-kafka/pull/64) Manual commits support
- [PR #68](https://github.com/monix/monix-kafka/pull/68) Scalafmt
- Updates of dependencies

Depends on Monix `3.0.0-RC2`.


## Version 1.0.0-RC2 (Nov 15, 2018)

- Updates Monix to 3.0.0-RC2
- [PR #44](https://github.com/monix/monix-kafka/pull/44) - Add possibility of adding custom configs
- [PR #48](https://github.com/monix/monix-kafka/pull/48) - Fix Kafka's Config classloader conflicts

## Version 0.17 (Nov 8, 2018)

- [PR #45](https://github.com/monix/monix-kafka/pull/45)
- This version uses Monix 2.3.x

## Version 1.0.0-RC1 (Mar 23, 2018)

- Updates Monix to 3.0.0-RC1
- Updates Kafka to 1.0.1
- Adds tests using Embedded Kafka

## Version 1.0.0-M3 (Feb 8, 2018)

- Updates Monix to 3.0.0-M3

## Version 0.16 (Feb 6, 2018)

- Adds [Kip62](https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread) config parameters and default values to Kafka 1.0.0 `default.conf`.
- Adds configs for metrics

## Version 0.15 (Jan 31, 2018)

- Updates Monix to 2.3.3
- Updates Scala to 2.11.12 and 2.12.4
- Added support for Kafka 0.11.0.1 and 1.0.0

## Version 0.14 (May 3, 2017)

- Updates Monix to 2.3.0
- Updates Kafka 10 to 0.10.2.1
- Updates Scala to 2.11.11 and 2.12.2

## Version 0.13 (Mar 27, 2017)

- Updates Monix to 2.2.4
- Activate Scala 2.10 and 2.12 cross-builds

## Version 0.12 (Mar 10, 2017)

- Updates Monix to 2.2.3

## Version 0.11 (Feb 22, 2017)

- Updates Monix to 2.2.2

## Version 0.9 (Jan 25, 2016)

- Updates Monix to 2.2.0
- [Issue #5](https://github.com/monix/monix-kafka/pull/5):
  added new optional param `classInstance` to `Serializer` and `Deserializer` 

## Version 0.8 (Dec 19, 2016)

- Updates Monix to 2.1.2
- Updates the Kafka 10 client to 0.10.1.0
