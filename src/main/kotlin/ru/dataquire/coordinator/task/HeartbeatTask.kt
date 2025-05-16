package ru.dataquire.coordinator.task

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import ru.dataquire.coordinator.client.ExtractorClient
import ru.dataquire.coordinator.client.LoaderClient
import ru.dataquire.coordinator.configuration.CoordinatorConfiguration

@Component
class HeartbeatTask(
    private val loaderClient: LoaderClient,
    private val extractorClient: ExtractorClient,
    private val coordinatorConfiguration: CoordinatorConfiguration
) {
    private val logger: Logger = LoggerFactory.getLogger(HeartbeatTask::class.java)

    @Async
    @Scheduled(cron = "0 * * * * *")
    fun heartbeatExtractor() {
        logger.info("[HEARTBEAT] Started checking extractors")

        coordinatorConfiguration.extractors.forEach { extractor ->
            logger.debug("[HEARTBEAT] {}", extractor)
            val isHealth = extractorClient.health(extractor)
            if (isHealth.not()) {
                coordinatorConfiguration.removeExtractorAddress(extractor)
            }
            logger.info("[HEARTBEAT] Finished checking extractors")
        }
    }

    @Async
    @Scheduled(cron = "0 * * * * *")
    fun heartbeatLoader() {
        logger.info("[HEARTBEAT] Started checking loaders")

        coordinatorConfiguration.loaders.forEach { loader ->
            logger.debug("[HEARTBEAT] {}", loader)
            val isHealth = loaderClient.health(loader)
            if (isHealth.not()) {
                coordinatorConfiguration.removeLoaderAddress(loader)
            }
            logger.info("[HEARTBEAT] Finished checking loaders")
        }
    }
}