package ru.yandex.practicum.collector.services;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import ru.yandex.practicum.collector.models.hub.HubEvent;
import ru.yandex.practicum.collector.models.sensors.SensorEvent;

@Service
public class CollectorService {
    private final Producer<String, SpecificRecord> producer;
    private final EventMapper mapper;
    private final String sensorsTopic;
    private final String hubsTopic;

    public CollectorService(Producer<String, SpecificRecord> producer, EventMapper mapper,
            @Value("${collector.kafka.topics.sensors}") String sensorsTopic,
            @Value("${collector.kafka.topics.hubs}") String hubsTopic) {
        this.producer = producer;
        this.mapper = mapper;
        this.sensorsTopic = sensorsTopic;
        this.hubsTopic = hubsTopic;
    }

    public void sendHubEvent(HubEvent event) {
        send(hubsTopic, event.getHubId(), event.getTimestamp(), mapper.toAvro(event));
    }

    public void sendSensorEvent(SensorEvent event) {
        send(sensorsTopic, event.getHubId(), event.getTimestamp(), mapper.toAvro(event));
    }

    private void send(String topic, String hubId, Instant timestamp, SpecificRecord event) {
        try {
            producer.send(new ProducerRecord<>(topic, null, timestamp.toEpochMilli(), hubId, event)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka send interrupted", e);
        } catch (ExecutionException | KafkaException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Cannot publish event to Kafka", e);
        }
    }
}
