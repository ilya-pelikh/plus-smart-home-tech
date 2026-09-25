package ru.yandex.practicum.collector.kafka;

import jakarta.annotation.PreDestroy;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ManagedKafkaProducer extends KafkaProducer<String, SpecificRecord> {
    public ManagedKafkaProducer(Map<String, Object> configs,
            Serializer<String> keySerializer,
            Serializer<SpecificRecord> valueSerializer) {
        super(configs, keySerializer, valueSerializer);
    }

    @PreDestroy
    public void flushBeforeClose() {
        flush();
    }
}
