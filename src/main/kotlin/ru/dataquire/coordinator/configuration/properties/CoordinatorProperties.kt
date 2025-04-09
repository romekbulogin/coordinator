package ru.dataquire.coordinator.configuration.properties

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "coordinator")
class CoordinatorProperties(
    val producers: List<String>
)
