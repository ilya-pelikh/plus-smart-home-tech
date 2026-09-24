package ru.yandex.practicum.collector.kafka;

import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfiguration {
    @Bean(destroyMethod = "close")
    public Producer<String, SpecificRecord> telemetryProducer(
            @Value("${collector.kafka.bootstrap-servers}") String bootstrapServers) {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000
        ), new StringSerializer(), new AvroSerializer());
    }
}
