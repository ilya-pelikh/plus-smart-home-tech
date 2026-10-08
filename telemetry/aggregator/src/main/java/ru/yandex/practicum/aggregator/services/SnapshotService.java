package ru.yandex.practicum.aggregator.services;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.springframework.stereotype.Service;

import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

@Service
public class SnapshotService {
    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(event.getHubId(), hubId -> {
            return new SensorsSnapshotAvro(
                    hubId,
                    event.getTimestamp(),
                    new HashMap<>());
        });

        SensorStateAvro oldState = snapshot.getSensorsState().get(event.getId());

        if (oldState != null
                && (event.getTimestamp().isBefore(oldState.getTimestamp())
                        || Objects.equals(event.getPayload(), oldState.getData()))) {
            return Optional.empty();
        }

        SensorStateAvro newState = new SensorStateAvro(event.getTimestamp(), event.getPayload());

        snapshot.getSensorsState().put(event.getId(), newState);
        snapshot.setTimestamp(event.getTimestamp());

        return Optional.of(snapshot);
    }
}