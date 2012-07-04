kafka-hadoop-consumer
=====================

Another kafka-hadoop-consumer

## Quick Start
    $ mvn start
    $ java -cp target/hadoop_consumer-1.0-SNAPSHOT.jar:`hadoop classpath` kafka.consumer.HadoopConsumer -z <zookeeper> -t <topic> target_hdfs_path

