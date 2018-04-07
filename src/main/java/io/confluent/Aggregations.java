package io.confluent;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class Aggregations {

  private static final Logger LOG = LoggerFactory.getLogger(Aggregations.class);

  static class OrderData {
    long orderTimeMs;
    int orderId;
    String itemName;
    double orderValue;

    OrderData(long orderTimeMs, int orderId, String itemName, double orderValue) {
      this.orderTimeMs = orderTimeMs;
      this.orderId = orderId;
      this.itemName = itemName;
      this.orderValue = orderValue;
    }

    static OrderData fromDelimitedString(String value) {
      String[] values = value.split(",");
      return new OrderData(Long.parseLong(values[0]),
                           Integer.parseInt(values[1]),
                           values[2],
                           Double.parseDouble(values[3]));
    }
  }


  public static void main(String args[]){

    LOG.debug("starting");
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "apurva-aggregations-test-0");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                   Serdes.String().getClass().getName());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                   Serdes.String().getClass().getName());

    // create table foo as select orderid, sum(order_total)/count(*) from orders group by 'constant
    // key'

    StreamsBuilder builder = new StreamsBuilder();

    KStream<byte[], String> inputStream = builder.stream("orders",
                                                          Consumed.with(Serdes.ByteArray(),
                                                                        Serdes.String()));

    KTable<String, Double> aggregateTable = inputStream
        .groupBy((key, value) -> "0")
        .aggregate(
            () -> 0.0,
            (String key, String value, Double currentValue) -> {
              OrderData data = OrderData.fromDelimitedString(value);
              return currentValue + data.orderValue;
            },
            Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("order-totals")
                .withValueSerde(Serdes.Double())
        );

    KStream<String, String> outputStream = aggregateTable.toStream()
        .mapValues(value -> String.valueOf(value));
    outputStream.to("order-totals");


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
      latch.await();
    } catch (Exception e) {
      System.exit(1);
    }

    System.exit(0);

  }
}
