package ru.dataquire.coordinator.configuration

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.dataquire.coordinator.configuration.properties.CoordinatorProperties
import ru.dataquire.coordinator.dto.ExtractorAddress
import ru.dataquire.coordinator.service.ExtractorClient

@Configuration
class CoordinatorConfiguration(
    coordinatorProperties: CoordinatorProperties,
    private val extractorClient: ExtractorClient,
) {
    private val logger: Logger = LoggerFactory.getLogger(CoordinatorConfiguration::class.java)

    val extractors: MutableList<ExtractorAddress> = mutableListOf()

    init {
        coordinatorProperties.producers.forEach { producerAddress ->
            logger.debug("[CHECK TO UPPING] {}", producerAddress)
            val address = getAddress(producerAddress)
            val isHealth = extractorClient.health(address)
            if (isHealth) {
                registrationExtractorAddress(address)
            }
        }
    }

    @Bean
    fun mapper() = jacksonObjectMapper().apply {
        configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        registerModule(JavaTimeModule())
    }

    @Synchronized
    fun registrationExtractorAddress(address: ExtractorAddress) = extractors.add(address)

    @Synchronized
    fun removeExtractorAddress(address: ExtractorAddress) = extractors.remove(address)

    private fun getAddress(address: String): ExtractorAddress {
        val splitAddress = address.split(":", limit = 2)
        return ExtractorAddress(
            host = splitAddress.first(),
            port = splitAddress.last().toInt()
        )
    }
}
