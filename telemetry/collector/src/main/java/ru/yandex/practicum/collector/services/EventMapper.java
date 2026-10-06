package ru.yandex.practicum.collector.services;

import java.time.Instant;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.models.hub.*;
import ru.yandex.practicum.collector.models.sensors.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Component
public class EventMapper {
    public SensorEventAvro toAvro(SensorEventProto event) {
        Instant timestamp = Instant.ofEpochSecond(
                event.getTimestamp().getSeconds(),
                event.getTimestamp().getNanos());

        Object payload = switch (event.getPayloadCase()) {
            case CLIMATE_SENSOR -> {
                var e = event.getClimateSensor();
                yield new ClimateSensorAvro(e.getTemperatureC(), e.getHumidity(), e.getCo2Level());
            }
            case LIGHT_SENSOR -> {
                var e = event.getLightSensor();
                yield new LightSensorAvro(e.getLinkQuality(), e.getLuminosity());
            }
            case MOTION_SENSOR -> {
                var e = event.getMotionSensor();
                yield new MotionSensorAvro(e.getLinkQuality(), e.getMotion(), e.getVoltage());
            }
            case SWITCH_SENSOR -> new SwitchSensorAvro(event.getSwitchSensor().getState());
            case TEMPERATURE_SENSOR -> {
                var e = event.getTemperatureSensor();
                yield new TemperatureSensorAvro(event.getId(), event.getHubId(), timestamp,
                        e.getTemperatureC(), e.getTemperatureF());
            }
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Sensor payload is required");
        };
        return new SensorEventAvro(event.getId(), event.getHubId(), timestamp, payload);
    }

    public HubEventAvro toAvro(HubEventProto event) {
        Instant timestamp = Instant.ofEpochSecond(
                event.getTimestamp().getSeconds(),
                event.getTimestamp().getNanos());

        Object payload = switch (event.getPayloadCase()) {
            case DEVICE_ADDED -> {
                DeviceAddedEventProto ev = event.getDeviceAdded();
                yield new DeviceAddedEventAvro(ev.getId(), DeviceTypeAvro.valueOf(ev.getType().name()));
            }
            case DEVICE_REMOVED -> {
                yield new DeviceRemovedEventAvro(event.getDeviceRemoved().getId());
            }
            case SCENARIO_ADDED -> {
                ScenarioAddedEventProto ev = event.getScenarioAdded();
                yield new ScenarioAddedEventAvro(ev.getName(),
                        ev.getConditionList().stream().map(this::toAvro).toList(),
                        ev.getActionList().stream().map(this::toAvro).toList());
            }
            case SCENARIO_REMOVED -> {
                yield new ScenarioRemovedEventAvro(event.getScenarioRemoved().getName());
            }
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Hub payload is required");
        };

        return new HubEventAvro(event.getHubId(), timestamp, payload);
    }

    private ScenarioConditionAvro toAvro(ScenarioConditionProto condition) {
        Object value = switch (condition.getType()) {
            case MOTION, SWITCH -> {
                if (condition.getValueCase() != ScenarioConditionProto.ValueCase.BOOL_VALUE) {
                    throw new IllegalArgumentException("Boolean condition value is required");
                }
                yield condition.getBoolValue();
            }
            case LUMINOSITY, TEMPERATURE, CO2LEVEL, HUMIDITY -> {
                if (condition.getValueCase() != ScenarioConditionProto.ValueCase.INT_VALUE) {
                    throw new IllegalArgumentException("Integer condition value is required");
                }
                yield condition.getIntValue();
            }
            case UNRECOGNIZED -> throw new IllegalArgumentException("Unknown condition type");
        };
        return new ScenarioConditionAvro(condition.getSensorId(),
                ConditionTypeAvro.valueOf(condition.getType().name()),
                ConditionOperationAvro.valueOf(condition.getOperation().name()), value);
    }

    private DeviceActionAvro toAvro(DeviceActionProto action) {
        return new DeviceActionAvro(action.getSensorId(), ActionTypeAvro.valueOf(action.getType().name()),
                action.hasValue() ? action.getValue() : null);
    }

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
        Object value = switch (condition.getType()) {
            case MOTION, SWITCH -> condition.getValue() != 0;
            case LUMINOSITY, TEMPERATURE, CO2LEVEL, HUMIDITY -> condition.getValue();
        };
        return new ScenarioConditionAvro(condition.getSensorId(),
                ConditionTypeAvro.valueOf(condition.getType().name()),
                ConditionOperationAvro.valueOf(condition.getOperation().name()), value);
    }

    private DeviceActionAvro toAvro(DeviceAction action) {
        return new DeviceActionAvro(action.getSensorId(),
                ActionTypeAvro.valueOf(action.getType().name()), action.getValue());
    }
}
