pentaho-kafka-producer
======================

Apache Kafka producer step plug-in for Pentaho Kettle.

[![Build Status](https://travis-ci.org/RuckusWirelessIL/pentaho-kafka-producer.png)](https://travis-ci.org/RuckusWirelessIL/pentaho-kafka-producer)


### Screenshots ###

![Using Apache Kafka Producer in Kettle](https://raw.github.com/RuckusWirelessIL/pentaho-kafka-producer/master/doc/example.png)


### Apache Kafka Compatibility ###

The producer depends on Apache Kafka 0.8.1.1, which means that the broker must be of 0.8.x version or later.


### Installation ###

1. Download ```pentaho-kafka-producer``` Zip archive from [latest release page](https://github.com/RuckusWirelessIL/pentaho-kafka-producer/releases/latest).
2. Extract downloaded archive into *plugins/steps* directory of your Pentaho Data Integration distribution.


### Building from source code ###

```
mvn clean package
```
