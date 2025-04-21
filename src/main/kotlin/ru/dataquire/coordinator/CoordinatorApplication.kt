package ru.dataquire.coordinator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling
import ru.dataquire.coordinator.configuration.properties.CoordinatorProperties

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(
    value = [
        CoordinatorProperties::class
    ]
)
class CoordinatorApplication

fun main(args: Array<String>) {
    runApplication<CoordinatorApplication>(*args)
}
