package ru.yandex.practicum.collector.services;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.models.hub.*;
import ru.yandex.practicum.collector.models.sensors.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Component
public class EventMapper {
    public SensorEventAvro toAvro(SensorEvent event) {
        Object payload = switch (event) {
            case ClimateSensorEvent e -> new ClimateSensorAvro(e.getTemperatureC(), e.getHumidity(), e.getCo2Level());
            case LightSensorEvent e -> new LightSensorAvro(e.getLinkQuality(), e.getLuminosity());
            case MotionSensorEvent e -> new MotionSensorAvro(e.getLinkQuality(), e.getMotion(), e.getVoltage());
            case SwitchSensorEvent e -> new SwitchSensorAvro(e.getState());
            case TemperatureSensorEvent e -> new TemperatureSensorAvro(
                    e.getId(), e.getHubId(), e.getTimestamp(), e.getTemperatureC(), e.getTemperatureF());
            default -> throw new IllegalArgumentException("Unsupported sensor event: " + event.getClass());
        };
        return new SensorEventAvro(event.getId(), event.getHubId(), event.getTimestamp(), payload);
    }

    public HubEventAvro toAvro(HubEvent event) {
        Object payload = switch (event) {
            case DeviceAddedEvent e -> new DeviceAddedEventAvro(
                    e.getId(), DeviceTypeAvro.valueOf(e.getDeviceType().name()));
            case DeviceRemovedEvent e -> new DeviceRemovedEventAvro(e.getId());
            case ScenarioAddedEvent e -> new ScenarioAddedEventAvro(e.getName(),
                    e.getConditions().stream().map(this::toAvro).toList(),
                    e.getActions().stream().map(this::toAvro).toList());
            case ScenarioRemovedEvent e -> new ScenarioRemovedEventAvro(e.getName());
            default -> throw new IllegalArgumentException("Unsupported hub event: " + event.getClass());
        };
        return new HubEventAvro(event.getHubId(), event.getTimestamp(), payload);
    }

    private ScenarioConditionAvro toAvro(ScenarioCondition condition) {
        return new ScenarioConditionAvro(condition.getSensorId(),
                ConditionTypeAvro.valueOf(condition.getType().name()),
                ConditionOperationAvro.valueOf(condition.getOperation().name()), condition.getValue());
    }

    private DeviceActionAvro toAvro(DeviceAction action) {
        return new DeviceActionAvro(action.getSensorId(),
                ActionTypeAvro.valueOf(action.getType().name()), action.getValue());
    }
}
