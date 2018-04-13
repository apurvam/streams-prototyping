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
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


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
    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    // The topology below executes the following ksql query, where we have a single field of
    // interest, 'order_total', and we compute the average of the order total and double
    // the order total.
    //
    // create table foo as select \
    //   constant_key, avg(order_total * 2), avg(order_total) from orders  \
    //   group by constant_key

    StreamsBuilder builder = new StreamsBuilder();

    KStream<byte[], String> inputStream = builder.stream("orders",
                                                          Consumed.with(Serdes.ByteArray(),
                                                                        Serdes.String()));

    KTable<Windowed<String>, String> aggregateTable = inputStream
        .mapValues((String value) -> {
          // In this stage, we insert the double order total and drop all the other fields.
          OrderData data = OrderData.fromDelimitedString(value);
          StringBuilder stringBuilder = new StringBuilder();
          stringBuilder.append(data.orderValue * 2);
          stringBuilder.append(',');
          stringBuilder.append(data.orderValue);
          return stringBuilder.toString();
        })
        // here we reroute everything to a single topic by assigning a static key.
        .groupBy((key, value) -> "0")
        // Bucket the orders into 30 second windows.
        .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(30)))
        // here we compute the two sums and a count.
        .aggregate(
            () -> "",
            (String key, String value, String currentValue) -> {
              List<Double> values = Arrays.stream(value.split(","))
                  .map(Double::parseDouble)
                  .collect(Collectors.toList());

              LOG.debug("values: {}", values.toString());
              LOG.debug("current value: {}", currentValue);
              List<Double> currentValues;
              if (!currentValue.contains(",")) {
                currentValues = Arrays.asList(0.0, 0.0, 0.0);
              } else {
                currentValues = Arrays.stream(currentValue.split(","))
                    .map(Double::parseDouble)
                    .collect(Collectors.toList());
              }

              LOG.debug("current values: {}", currentValues.toString());

              Double first = currentValues.get(0) + values.get(0);
              Double second = currentValues.get(1) + values.get(1);
              int count = currentValues.get(2).intValue() + 1;

              return String.valueOf(first)
                     + "," + String.valueOf(second) + ","
                     + String.valueOf(count);
            },
            Materialized.as("order-totals")
        )
        // now we do the division between the sums and the count to get the two averages.
        .mapValues((String value) -> {
          List<Double> values = Arrays.stream(value.split(","))
              .map(Double::parseDouble)
              .collect(Collectors.toList());
          int count = values.get(2).intValue();
          Double firstAvg = values.get(0) / count;
          Double secondAvg = values.get(1) / count;
          return firstAvg.toString() + "," + secondAvg.toString();
        });

    KStream<String, String> outputStream =
        aggregateTable
            .toStream((Windowed<String> key, String value) -> String.valueOf(key.window().start()) +
                                                              "-" +
                                                              String.valueOf(key.window().end()) +
                                                              "-" +
                                                              key.key()
            );
    outputStream.to("order-averages");

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
