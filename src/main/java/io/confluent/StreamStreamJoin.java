package io.confluent;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StreamStreamJoin {

  private static final Logger LOG = LoggerFactory.getLogger(StreamStreamJoin.class);

  /**
   * This models the following KSQL program:
   * CREATE STREAM impressions (impressiontime bigint, impressionid varchar, userid varchar, adid \
   *   varchar) WITH (kafka_topic='impressions', value_format='delimited');
   *
   * CREATE STREAM clicks (clicktime bigint, userid varchar, impressionid varchar, adid varchar, \
   *   clickid varchar) WITH (kafka_topic='clicks', value_format='delimited');
   *
   * CREATE STREAM ads_with_clicks AS SELECT impression.impressionid as impid, impression.userid \
   *   as userid, impression.adid as adid, clicks.clickid as clickid FROM impressions LEFT JOIN \
   *   clicks ON impressions.impressionid = clicks.impressionid WINDOW TUMBLING (size 30 second) \
   *   GROUP BY adid;
   *
   * # The following computes the number of clicks divided by the number of impressions for an ad.
   * CREATE TABLE ctr AS SELECT adid, (count(*) WHERE clickid != NULL) / count(*) FROM \
   *   ads_with_clicks
   *
   * You can create the 'impressions' and 'clicks' data by running the ksql datagen tool as follows
   *
   * ./ksql-datagen schema=impressions.avro format=delimited topic=impressions key=impressionid
   * ./ksql-datagen schema=clicks.avro format=delimited topic=clicks key=clickid
   * @param args
   */

  public static void main(String args[]) {
    LOG.debug("starting");
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "apurva-stream-stream-join");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                   Serdes.String().getClass().getName());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                   Serdes.String().getClass().getName());

    StreamsBuilder builder = new StreamsBuilder();

    KStream<byte[], String> impressions = builder.stream("impressions",
                                                         Consumed.with(Serdes.ByteArray(),
                                                                       Serdes.String()));


    KStream<byte[], String> clicks = builder.stream("clicks",
                                                    Consumed.with(Serdes.ByteArray(),
                                                                  Serdes.String()));

    KStream<String, String> clicksbyImpression =
        clicks.map((byte[] key, String value) -> {
          Click data = Click.fromDelimitedString(value);
          return new KeyValue<>(data.impressionId, value);
        });

    KStream<String, String > adsWithClicks = impressions
        // convert the key to a string
        .map((byte[] key, String value) -> {
          Impression impression = Impression.fromDelimitedString(value);
          return new KeyValue<>(impression.impressionId, value);
        })
        // join impressions with clicks to produce a unified stream
        .leftJoin(clicksbyImpression,
              (String impressionString, String clickString) -> {
                Impression impression = Impression.fromDelimitedString(impressionString);
                ImpressionAndClick impAndClick =
                    new ImpressionAndClick(impression.impressionId, impression.userId,
                                           impression.adId, false);
                if (clickString != null) {
                  impAndClick.wasClicked = true;
                }
                return impAndClick.toDelimitedString();
              }, JoinWindows.of(TimeUnit.SECONDS.toMillis(60)))
        // rekey by adId, now we can compute the CTR per ad.
        .map((String key, String value) -> {
          ImpressionAndClick impressionAndClick = ImpressionAndClick.fromDelimitedString(value);
          return new KeyValue<>(impressionAndClick.adId, value);
        });


    // adsWithClicks is now a stream of [adId] -> [impressionId, userId, adId, clicked]

    KTable<String, String> clickThroughRate = adsWithClicks
        .groupByKey()
        .aggregate(
            () -> "",
            (String key, String newValue, String currentValue) -> {
              ImpressionAndClick click = ImpressionAndClick.fromDelimitedString(newValue);
              List<Integer> current;
              if (currentValue.contains(","))  {
                current = Arrays.stream(currentValue.split(","))
                    .map(Integer::valueOf)
                    .collect(Collectors.toList());
              } else {
                current = Arrays.asList(0, 0);
              }
              if (click.wasClicked) {
                int numClicks = current.get(1);
                numClicks += 1;
                current.set(1, numClicks);
              } else {
                int numImpressions = current.get(0);
                numImpressions += 1;
                current.set(0, numImpressions);
              }
              return current.get(0) + "," + current.get(1);
            },
            Materialized.as("click-through-rate")
        )
        .mapValues((String value) -> {
          List<Integer> current = Arrays.stream(value.split(","))
              .map(Integer::valueOf)
              .collect(Collectors.toList());
          int numImpressions = current.get(0);
          int numClicks = current.get(1);
          if (numClicks == 0) {
            return "0.0";
          }
          double ctr = (double) numClicks / numImpressions;
          return String.valueOf(ctr);
        });

    clickThroughRate.toStream().to("click-through-rate");

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

  static class Click {
    long clickTime;
    String userId;
    String impressionId;
    String adId;
    String clickId;

    Click(long clickTime, String userId, String impressionId, String adId, String clickId) {
      this.clickTime = clickTime;
      this.userId = userId;
      this.impressionId = impressionId;
      this.adId = adId;
      this.clickId = clickId;
    }

    static Click fromDelimitedString(String clicks) {
      String[] data = clicks.split(",");
      return new Click(Long.valueOf(data[0]), data[1], data[2], data[3], data[4]);
    }
  }

  static class Impression {
    long impressionTime;
    String impressionId;
    String userId;
    String adId;

    Impression(long impressionTime, String impressionId, String userId, String adId) {
      this.impressionTime = impressionTime;
      this.impressionId = impressionId;
      this.userId = userId;
      this.adId = adId;
    }

    static Impression fromDelimitedString(String value) {
      String[] data = value.split(",");
      return new Impression(Long.valueOf(data[0]), data[1], data[2], data[3]);
    }
  }

  static class ImpressionAndClick {
    String impressionId;
    String userId;
    String adId;
    boolean wasClicked;

    ImpressionAndClick(String impressionId, String userId, String adId, boolean wasClicked) {
      this.impressionId = impressionId;
      this.userId = userId;
      this.adId = adId;
      this.wasClicked = wasClicked;
    }

    static ImpressionAndClick fromDelimitedString(String value) {
      String[] data = value.split(",");
      return new ImpressionAndClick(data[0], data[1], data[2], Boolean.valueOf(data[3]));
    }

    String toDelimitedString() {
      return this.impressionId + "," + this.userId + "," + this.adId + ","
             + String.valueOf(this.wasClicked);
    }
  }
}
