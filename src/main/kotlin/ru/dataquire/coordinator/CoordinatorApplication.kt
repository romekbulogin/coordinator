package ru.dataquire.coordinator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import ru.dataquire.coordinator.configuration.properties.CoordinatorProperties

@SpringBootApplication
@EnableConfigurationProperties(
    value = [
        CoordinatorProperties::class
    ]
)
class CoordinatorApplication

fun main(args: Array<String>) {
    runApplication<CoordinatorApplication>(*args)
}
