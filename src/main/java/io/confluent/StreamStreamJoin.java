package io.confluent;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamStreamJoin {

  public static final Logger LOG = LoggerFactory.getLogger(StreamStreamJoin.class);

  public static void main(String args[]) {
    LOG.debug("starting");
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "apurva-stream-stream-join");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                   Serdes.String().getClass().getName());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                   Serdes.String().getClass().getName());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> source = builder.stream("input-stream-1");
    KStream<String, String> filtered = source.filter((k, v) -> v.startsWith("apurva"));
    filtered.to("filtered-output");

    final KafkaStreams streams = new KafkaStreams(builder.build(), properties);


    final CountDownLatch latch = new CountDownLatch(1);

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
      @Override
      public void run() {
        streams.close();
        latch.countDown();
      }
    });

    try {
          streams.start();
    } catch (Exception e) {
      System.exit(1);
    }

    System.exit(0);

  }
}
