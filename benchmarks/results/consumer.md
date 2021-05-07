
## Consumer Benchmarks

The consumer benchmark covers the *manual* and *auto commit* consumer implementations of the different libraries.
The manual commit will also cover producing committing back the consumed offsets.
It also runs on different range `pollHeartbeatRate` [1, 10, 15, 100, 1000], which is an important configuration
 implemented in this `monix-kafka` library.

## RC7
### 1fork 1thread
Benchmark                               Mode  Cnt   Score   Error  Units
ConsumerBenchmark.monix_auto_commit_async  thrpt    7  13.097 ± 0.827  ops/s
ConsumerBenchmark.monix_auto_commit_sync   thrpt    7  12.486 ± 1.087  ops/s
ConsumerBenchmark.monix_manual_commit  thrpt   10  11.519 ± 2.247  ops/s

### 1 fork 3 threads
Benchmark                               Mode  Cnt   Score   Error  Units
ConsumerBenchmark.monix_auto_commit    thrpt   10  16.186 ± 0.920  ops/s
ConsumerBenchmark.monix_manual_commit  thrpt   10  16.319 ± 1.465  ops/s

## RC8 - (Introduces PollHeartbeatRate)
### 1fork 1thread
---
Benchmark                                               Mode  Cnt   Score   Error  Units
ConsumerBenchmark.monixAsyncAutoCommitHeartbeat15ms  thrpt   7  13.126 ± 1.737  ops/s
ConsumerBenchmark.monixSyncAutoCommitHeartbeat15ms   thrpt   7  12.102 ± 1.214  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat1000ms  thrpt    7   2.978 ± 0.006  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat100ms   thrpt    7   9.961 ± 1.317  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat10ms    thrpt    7  13.346 ± 0.716  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat15ms    thrpt    7  13.454 ± 2.680  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat1ms     thrpt    7  14.281 ± 1.591  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat50ms    thrpt    7  11.900 ± 0.698  ops/s
---
### 1 fork 3 threads 
Benchmark                                               Mode  Cnt   Score   Error  Units
ConsumerBenchmark.monixAsyncAutoCommitHeartbeat15ms  thrpt   7  16.966 ± 2.659  ops/s
ConsumerBenchmark.monixSyncAutoCommitHeartbeat15ms   thrpt   7  15.083 ± 4.242  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat1000ms  thrpt    7   2.978 ± 0.006  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat100ms   thrpt    7   9.961 ± 1.317  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat10ms    thrpt    7  13.346 ± 0.716  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat15ms    thrpt    7  13.454 ± 2.680  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat1ms     thrpt    7  14.281 ± 1.591  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat50ms    thrpt    7  11.900 ± 0.698  ops/s

```sbt
sbt 'benchmarks/jmh:run -i 5 -wi 1 -f1 -t1 monix.kafka.benchmarks.ConsumerBenchmark.*'
```