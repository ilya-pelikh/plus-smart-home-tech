package ru.yandex.practicum.aggregator;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.aggregator.services.SnapshotService;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private volatile boolean running = true;

    private final Consumer<String, SensorEventAvro> consumer;
    private final Producer<String, SpecificRecord> producer;
    private final SnapshotService snapshotService;

    public void start() {
        try {
            consumer.subscribe(List.of("telemetry.sensors.v1"));
            while (running) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    Optional<SensorsSnapshotAvro> snapshot = snapshotService.updateState(record.value());
                    if (snapshot.isPresent()) {
                        SensorsSnapshotAvro updatedSnapshot = snapshot.get();
                        producer.send(new ProducerRecord<String, SpecificRecord>(
                                "telemetry.snapshots.v1",
                                updatedSnapshot.getHubId(),
                                updatedSnapshot)).get();
                    }
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            log.info("Останавливаем агрегацию");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Поток агрегации прерван");
        } catch (Exception e) {
            log.error("Ошибка обработки событий", e);
        } finally {
            try {
                producer.flush();
            } finally {
                try {
                    consumer.close();
                } finally {
                    producer.close();
                }
            }
        }
    }

    @EventListener(ContextClosedEvent.class)
    public void stop() {
        running = false;
        consumer.wakeup();
    }
}