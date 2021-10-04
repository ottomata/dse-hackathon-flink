# RocksDB & state processor API test case

To reproduce:
* compile this project using `./mvnw package`
* download flink 1.14.0
* adjust flink settings:
```yaml
taskmanager.memory.process.size: 4g
taskmanager.numberOfTaskSlots: 4
```
* start 3 TM
```shell
./bin/start-cluster.sh && ./bin/start-cluster.sh && ./bin/start-cluster.sh
```

## Create a state with 30M entries with the [state processor API](./src/main/scala/org/wikimedia/flink/CreateState.scala)
```shell
./bin/flink run -p 12 -c org.wikimedia.flink.CreateState $PATH_TO_DEV/rocksdb-state-processor-test/target/rocksdb-state-processor-test-1.0-SNAPSHOT-jar-with-dependencies.jar --savepoint file:///tmp/flink-savepoint-1 --size 30000000
```

```
Job has been submitted with JobID 013a64204992e6d99745d84ad71547ea
Program execution finished
Job with JobID 013a64204992e6d99745d84ad71547ea has finished.
Job Runtime: 16106 ms
```

**16 seconds**

## Read all the keys using a [streaming job](./src/main/scala/org/wikimedia/flink/StreamJob.scala)
```shell
./bin/flink run -s file:///tmp/flink-savepoint-1 -p 12 -c org.wikimedia.flink.StreamJob $PATH_TO_DEV/rocksdb-state-processor-test/target/rocksdb-state-processor-test-1.0-SNAPSHOT-jar-with-dependencies.jar --size 30000000
Job has been submitted with JobID 285caa3812bddf3f5dfaaa7c491897a0
```
```
Program execution finished
Job with JobID 285caa3812bddf3f5dfaaa7c491897a0 has finished.
Job Runtime: 28566 ms
```

**28 seconds**

## Read all the keys using the [state processor API](./src/main/scala/org/wikimedia/flink/ReadState.scala)
```shell
./bin/flink run -p 12 -c org.wikimedia.flink.ReadState $PATH_TO_DEV/rocksdb-state-processor-test/target/rocksdb-state-processor-test-1.0-SNAPSHOT-jar-with-dependencies.jar --savepoint file:///tmp/flink-savepoint-1
```
```
Job has been submitted with JobID c51cd2198c2766107a17c04956b0c6c3
Program execution finished
Job with JobID c51cd2198c2766107a17c04956b0c6c3 has finished.
Job Runtime: 1582210 ms
Accumulator Results: 
- d2275ca8317cadd925c7ad438ecd6bec (java.lang.Long): 30000000
```

**43 minutes**
