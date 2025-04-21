package ru.dataquire.coordinator.service

import jakarta.servlet.http.HttpServletRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.dataquire.coordinator.configuration.CoordinatorConfiguration
import ru.dataquire.coordinator.dto.ExtractorAddress
import ru.dataquire.coordinator.dto.request.ExtractRequest
import ru.dataquire.coordinator.dto.request.MigrateRequest
import ru.dataquire.coordinator.dto.response.MigrateResponse
import java.sql.SQLException
import kotlin.random.Random

@Service
class CoordinatorService(
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
            val extractorId = Random(1337).nextInt(0, extractorCount)
            val extractor = coordinatorConfiguration.extractors[extractorId]

            val tableNames = tables.map { it.name }
            tableNames.forEach { tableName ->
                val extractRequest = ExtractRequest(
                    table = tableName,
                    dataSource = request.origin
                )
                extractorClient.extract(extractor, extractRequest)
            }
            logger.info("[MIGRATE] Finished migration and extracting: $extractor")
            MigrateResponse("Task migration created", tableNames)
        } catch (ex: SQLException) {
            logger.error("[MIGRATE] request={}. Failed message={}", request, ex.localizedMessage)
            MigrateResponse("Failed: ${ex.localizedMessage}", emptyList())
        }
    }

    fun registration(request: HttpServletRequest) {
        logger.info("[REGISTRATION] {}:{}", request.remoteAddr, request.remotePort)

        val xRealIp = request.getHeader("X-Real-IP")
        val xForwardedFor = request.getHeader("X-Forwarded-For")
        val remoteAddr = request.remoteAddr

        val xForwardedPort = request.getHeader("X-Forwarded-Port")
        val portFromBody = request.inputStream.bufferedReader().readLine()

        val host = listOf(xRealIp, xForwardedFor, remoteAddr).firstNotNullOf { it }
        val port = listOf(xForwardedPort, portFromBody).firstNotNullOf { it }.toInt()

        val extractorAddress = ExtractorAddress(host, port)
        logger.debug("[REGISTRATION] Defined address: {}", extractorAddress)
        coordinatorConfiguration.extractors.add(extractorAddress)
    }
}