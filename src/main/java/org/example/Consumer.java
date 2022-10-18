package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.logging.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {
    public static final String OUT_TOPIC = "topic2";
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);


    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "PLAINTEXT://localhost:9092");
        props.put("client.id", "consumer-group-1");
        props.put("group.id", "group");


        //Implicit offset commit by the consumer
        props.put("enable.auto.commit", "true");
        //Consume from the beginning of the topic, or only new messages, if no offset for group
        props.put("auto.offset.reset", "earliest");
        //Timeout for auto commit offset
        props.put("auto.commit.interval.ms", "1000");
        //how long consumer group waits for consumer, before drop it. in range with broker `group.min.session.timeout.ms`
        props.put("session.timeout.ms", "6000");
        //Heartbeat must be lower than session.timeout.ms, sent in separate HB thread
        props.put("heartbeat.interval.ms", "2000");
        //HB thread checks interval between poll() calls to check processing thread liveliness, send `leave-group`
        props.put("max.poll.interval.ms", "1000");
        //Never close idle channel
        props.put("connections.max.idle.ms", "-1");



        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props, Serdes.String().deserializer(), Serdes.String().deserializer());

        consumer.subscribe(List.of(OUT_TOPIC));
        logger.info("Subscribed to topic {}", OUT_TOPIC);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
            }
        }
    }
}