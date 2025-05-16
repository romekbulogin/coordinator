package ru.dataquire.coordinator.service

import jakarta.servlet.http.HttpServletRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.dataquire.coordinator.client.ExtractorClient
import ru.dataquire.coordinator.client.LoaderClient
import ru.dataquire.coordinator.configuration.CoordinatorConfiguration
import ru.dataquire.coordinator.dto.WorkerAddress
import ru.dataquire.coordinator.dto.request.ExtractRequest
import ru.dataquire.coordinator.dto.request.LoadRequest
import ru.dataquire.coordinator.dto.request.MigrateRequest
import ru.dataquire.coordinator.dto.response.MigrateResponse
import java.sql.SQLException
import kotlin.random.Random

@Service
class CoordinatorService(
    private val loaderClient: LoaderClient,
    private val schemeService: SchemeService,
    private val extractorClient: ExtractorClient,
    private val coordinatorConfiguration: CoordinatorConfiguration
) {
    private val logger: Logger = LoggerFactory.getLogger(CoordinatorService::class.java)

    fun migrate(request: MigrateRequest): MigrateResponse {
        return try {
            logger.info("[MIGRATE] Starting migration and extracting: $request")

            val tables = schemeService.migrate(request).tables

            val extractorCount = coordinatorConfiguration.extractors.size
            val extractorId = Random.nextInt(0, extractorCount)
            val extractor = coordinatorConfiguration.extractors[extractorId]

            val loaderCount = coordinatorConfiguration.loaders.size
            val loaderId = Random.nextInt(0, loaderCount)
            val loader = coordinatorConfiguration.loaders[loaderId]

            val tableNames = tables.map { it.name }
            tableNames.forEach { tableName ->
                val extractRequest = ExtractRequest(
                    table = tableName,
                    dataSource = request.origin
                )
                extractorClient.extract(extractor, extractRequest)
            }
            tableNames.forEach { tableName ->
                val loadRequest = LoadRequest(
                    table = tableName,
                    dataSource = request.recipient,
                    origin = request.origin,
                )
                loaderClient.load(loader, loadRequest)
            }
            logger.info("[MIGRATE] Finished migration and extracting: $extractor")
            MigrateResponse("Task migration created", tableNames)
        } catch (ex: SQLException) {
            logger.error("[MIGRATE] request={}. Failed message={}", request, ex.localizedMessage)
            MigrateResponse("Failed: ${ex.localizedMessage}", emptyList())
        }
    }

    fun registration(request: HttpServletRequest) {
        val workerType = request.getHeader("X-Worker-Type")
        logger.info(
            "[REGISTRATION] {}:{} workerType={}",
            request.remoteAddr,
            request.remotePort,
            workerType
        )

        val xRealIp = request.getHeader("X-Real-IP")
        val xForwardedFor = request.getHeader("X-Forwarded-For")
        val remoteAddr = request.remoteAddr

        val xForwardedPort = request.getHeader("X-Forwarded-Port")
        val portFromBody = request.inputStream.bufferedReader().readLine()

        val host = listOf(xRealIp, xForwardedFor, remoteAddr).firstNotNullOf { it }
        val port = listOf(xForwardedPort, portFromBody).firstNotNullOf { it }.toInt()

        val workerAddress = WorkerAddress(host, port)
        logger.debug("[REGISTRATION] Defined address: {}", workerAddress)
        when (workerType) {
            "extractor" -> coordinatorConfiguration.registrationExtractorAddress(workerAddress)
            "loader" -> coordinatorConfiguration.registrationLoaderAddress(workerAddress)
        }
    }
}