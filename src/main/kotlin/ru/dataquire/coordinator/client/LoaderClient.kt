package ru.dataquire.coordinator.client

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.client.RestClient
import org.springframework.web.client.RestClientException
import ru.dataquire.coordinator.dto.WorkerAddress
import ru.dataquire.coordinator.dto.request.ExtractRequest
import ru.dataquire.coordinator.dto.request.LoadRequest
import ru.dataquire.coordinator.dto.response.HealthResponse
import ru.dataquire.coordinator.dto.response.LoadResponse

private const val HEALTH_PATH = "/actuator/health"
private const val LOAD_PATH = "/v1/api/loader/load"

@Service
class LoaderClient {
    private val logger: Logger = LoggerFactory.getLogger(LoaderClient::class.java)

    private val restClient = RestClient.create()

    fun health(address: WorkerAddress): Boolean {
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

    fun load(address: WorkerAddress, request: LoadRequest): LoadResponse {
        return try {
            logger.info("[LOAD] {} in {}", request, address)
            val extractResponse = restClient.post()
                .uri("http://${address.host}:${address.port}$LOAD_PATH")
                .body(request)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .toEntity(LoadResponse::class.java)

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
            logger.error("[LOAD] {} in {}. Failed message={}", request.table, address, ex.localizedMessage)
            LoadResponse(isBegin = false, topicName = "", groupId = "")
        }
    }
}
