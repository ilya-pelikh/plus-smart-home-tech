package ru.yandex.practicum.collector.controller;

import lombok.RequiredArgsConstructor;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.collector.models.hub.HubEvent;
import ru.yandex.practicum.collector.models.sensors.SensorEvent;
import ru.yandex.practicum.collector.services.CollectorService;

import org.springframework.http.MediaType;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/events")
public class CollectorController {

    private final CollectorService collectorService;

    @PostMapping(value = "/sensors", consumes = MediaType.APPLICATION_JSON_VALUE)
    public void collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        try {
            collectorService.sendSensorEvent(event);
        } catch (ResponseStatusException e) {
            log.error("Failed to collect sensor event", e);
            throw e;
        }
    }

    @PostMapping(value = "/hubs", consumes = MediaType.APPLICATION_JSON_VALUE)
    public void collectHubEvent(@Valid @RequestBody HubEvent event) {
        try {
            collectorService.sendHubEvent(event);
        } catch (ResponseStatusException e) {
            log.error("Failed to collect hub event", e);
            throw e;
        }
    }
}
