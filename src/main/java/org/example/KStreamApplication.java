package org.example;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.stereotype.Component;

import java.util.Map;

@SpringBootApplication(scanBasePackages = {"org.example"})
@Configuration
@EnableKafka
@EnableKafkaStreams
public class KStreamApplication {
    public static void main(String[] args) {
        SpringApplication.run(KStreamApplication.class, args);
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs(@Value("${spring.application.name}") String appName) {
        return new KafkaStreamsConfiguration(
                Map.of(
                        StreamsConfig.APPLICATION_ID_CONFIG, appName,
                        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                        StreamsConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "-1",
                        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2",
                        CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG, "2000",
                        CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, "6000"

                )
        );
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer kStreamsFactoryConfigurer(KafkaStreamsInfrastructureCustomizer topologyProvider) {
        return factoryBean -> {
            factoryBean.setInfrastructureCustomizer(topologyProvider);
            factoryBean.setKafkaStreamsCustomizer((ks) -> {
                ks.cleanUp();
            });
        };
    }

    @Component
    public class KStreamInfraCustomizer implements KafkaStreamsInfrastructureCustomizer {

        public static final String SRC = "src";
        public static final String PRC = "prc";
        public static final String SNK = "snk";

        @Override
        public void configureTopology(Topology topology) {
            topology.addSource(SRC, Serdes.String().deserializer(), Serdes.String().deserializer(), Generator.IN_TOPIC)
                    .addProcessor(PRC, ReportProcessor::new, SRC)
                    .addSink(SNK, Consumer.OUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer(), PRC);
        }

        public static class ReportProcessor implements Processor<String, String, String, String> {
            private static final Logger logger = LoggerFactory.getLogger(ReportProcessor.class);
            private ProcessorContext<String, String> context;

            @Override
            public void init(ProcessorContext<String, String> context) {
                Processor.super.init(context);
                this.context = context;
            }

            @Override
            public void process(Record<String, String> record) {
                logger.info("Received record: {}", record);
                context.forward(record);
                System.exit(0);
            }
        }
    }
}