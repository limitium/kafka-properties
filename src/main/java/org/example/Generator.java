package org.example;//import util.properties packages

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;


public class Generator {
    private static final Logger logger = LoggerFactory.getLogger(Generator.class);
    public static final String IN_TOPIC = "topic1";
    private static int MAX_LENGTH = 20;

    public static void main(String[] args) {
        // create instance for properties to access producer configs
        Properties props = new Properties();

        props.put("bootstrap.servers", "PLAINTEXT://localhost:9092");


        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //Waits after 1st publish in ms to build a batch
        props.put("linger.ms", 0);



        Producer<String, String> producer = new KafkaProducer<>(props, Serdes.String().serializer(), Serdes.String().serializer());
        logger.info("Sending");
        final Random random = new Random();

        String key = randomString(random);
        String value = randomString(random);
        ProducerRecord<String, String> record = new ProducerRecord<>(IN_TOPIC, key, value);

        logger.info(record.toString());
        producer.send(record);
        logger.info("Message sent successfully");
        producer.close();
    }

    private static String randomString(Random random) {
        int leftLimit = 97;
        int rightLimit = 122;
        int length = random.nextInt(MAX_LENGTH + 1);

        return random.ints(leftLimit, rightLimit + 1)
                .limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }
}