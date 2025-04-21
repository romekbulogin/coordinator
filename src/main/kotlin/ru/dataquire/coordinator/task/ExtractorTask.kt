package ru.dataquire.coordinator.task

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import ru.dataquire.coordinator.configuration.CoordinatorConfiguration
import ru.dataquire.coordinator.service.ExtractorClient

@Component
class ExtractorTask(
    private val extractorClient: ExtractorClient,
    private val coordinatorConfiguration: CoordinatorConfiguration
) {
    private val logger: Logger = LoggerFactory.getLogger(ExtractorTask::class.java)

    @Scheduled(cron = "0 * * * * *")
    fun healthCheck() {
        logger.info("[HEALTH CHECK] Started checking")

        coordinatorConfiguration.extractors.forEach { extractor ->
            logger.debug("[HEALTH CHECK] {}", extractor)
            val isHealth = extractorClient.health(extractor)
            if (isHealth.not()) {
                coordinatorConfiguration.removeExtractorAddress(extractor)
            }
            logger.info("[HEALTH CHECK] Finished checking")
        }
    }
}