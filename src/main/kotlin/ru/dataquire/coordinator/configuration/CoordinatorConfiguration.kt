package ru.dataquire.coordinator.configuration

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.dataquire.coordinator.client.ExtractorClient
import ru.dataquire.coordinator.client.LoaderClient
import ru.dataquire.coordinator.configuration.properties.CoordinatorProperties
import ru.dataquire.coordinator.dto.WorkerAddress

@Configuration
class CoordinatorConfiguration(
    coordinatorProperties: CoordinatorProperties,
    private val loaderClient: LoaderClient,
    private val extractorClient: ExtractorClient,
) {
    private val logger: Logger = LoggerFactory.getLogger(CoordinatorConfiguration::class.java)

    val extractors: MutableList<WorkerAddress> = mutableListOf()
    val loaders: MutableList<WorkerAddress> = mutableListOf()

    init {
        coordinatorProperties.extractors.forEach { extractorAddress ->
            logger.debug("[HEALTHCHECK] extractor={}", extractorAddress)
            val address = getAddress(extractorAddress)
            val isHealth = extractorClient.health(address)
            if (isHealth) {
                registrationExtractorAddress(address)
            }
        }

        coordinatorProperties.loaders.forEach { loaderAddress ->
            logger.debug("[HEALTHCHECK] loader={}", loaderAddress)
            val address = getAddress(loaderAddress)
            val isHealth = loaderClient.health(address)
            if (isHealth) {
                registrationLoaderAddress(address)
            }
        }
    }

    @Bean
    fun mapper() = jacksonObjectMapper().apply {
        configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        registerModule(JavaTimeModule())
    }

    @Synchronized
    fun registrationExtractorAddress(address: WorkerAddress) = extractors.add(address)

    @Synchronized
    fun removeExtractorAddress(address: WorkerAddress) = extractors.remove(address)

    @Synchronized
    fun registrationLoaderAddress(address: WorkerAddress) = loaders.add(address)

    @Synchronized
    fun removeLoaderAddress(address: WorkerAddress) = loaders.remove(address)

    private fun getAddress(address: String): WorkerAddress {
        val splitAddress = address.split(":", limit = 2)
        return WorkerAddress(
            host = splitAddress.first(),
            port = splitAddress.last().toInt()
        )
    }
}
