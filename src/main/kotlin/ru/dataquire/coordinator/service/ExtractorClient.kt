package ru.dataquire.coordinator.service

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Service
import org.springframework.web.client.*
import ru.dataquire.coordinator.dto.ExtractorAddress
import ru.dataquire.coordinator.dto.request.ExtractRequest
import ru.dataquire.coordinator.dto.response.ExtractResponse
import ru.dataquire.coordinator.dto.response.HealthResponse

private const val HEALTH_PATH = "/actuator/health"
private const val EXTRACT_PATH = "/v1/api/extractor/extract"

@Service
class ExtractorClient {
    private val logger: Logger = LoggerFactory.getLogger(ExtractorClient::class.java)

    private val restClient = RestClient.create()

    fun health(address: ExtractorAddress): Boolean {
        return try {
            logger.info("[HEALTH] Trying to connect to {}", address)

            val healthResponse = restClient.get()
                .uri("http://${address.host}:${address.port}$HEALTH_PATH")
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .body(HealthResponse::class.java)

            checkNotNull(healthResponse)
            logger.debug("[HEALTH] {}", healthResponse)
            val isHealth = healthResponse.status == "UP"
            logger.info("[HEALTH] Successfully connected to $address")
            isHealth
        } catch (ex: RestClientException) {
            logger.warn("[CHECK TO UPPING] {} connection closed", address)
            false
        }
    }

    fun extract(address: ExtractorAddress, request: ExtractRequest): ExtractResponse {
        return try {
            logger.info("[EXTRACT] {} in {}", request.table, address)

            val extractResponse = restClient.post()
                .uri("http://${address.host}:${address.port}$EXTRACT_PATH")
                .body(request)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .toEntity(ExtractResponse::class.java)

            val body = checkNotNull(extractResponse.body)
            val isSuccess = extractResponse.statusCode.is2xxSuccessful
            logger.debug("[EXTRACT] {}", extractResponse.body)
            if (isSuccess) {
                logger.info("[EXTRACT] Successfully extracted {} in {}", request.table, address)
            } else {
                logger.warn("[EXTRACT] Failed to extract {} from {}", request.table, address)
            }
            body
        } catch (ex: RestClientException) {
            logger.error("[EXTRACT] {} in {}. Failed message={}", request.table, address, ex.localizedMessage)
            ExtractResponse("Failed: ${ex.localizedMessage}", 0)
        }
    }
}