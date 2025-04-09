package ru.dataquire.coordinator.configuration

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.dataquire.coordinator.configuration.properties.CoordinatorProperties

@Configuration
class CoordinatorConfiguration(
    private val coordinatorProperties: CoordinatorProperties
) {
    private val logger: Logger = LoggerFactory.getLogger(CoordinatorConfiguration::class.java)

    @PostConstruct
    fun checkToUpping() {
        coordinatorProperties.producers.forEach { producer ->
            logger.debug("[CHECK TO UPPING] {}", producer)
        }
    }

    @Bean
    fun mapper() = jacksonObjectMapper().apply {
        configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        registerModule(JavaTimeModule())
    }
}
