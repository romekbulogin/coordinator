package ru.dataquire.coordinator.controller

import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import ru.dataquire.coordinator.client.ExtractorClient
import ru.dataquire.coordinator.configuration.CoordinatorConfiguration
import ru.dataquire.coordinator.dto.request.ExtractRequest
import ru.dataquire.coordinator.dto.response.ExtractResponse
import kotlin.random.Random

@RestController
@RequestMapping("/v1/api/coordinator/extractor")
class ExtractController(
    private val extractorClient: ExtractorClient,
    private val coordinatorConfiguration: CoordinatorConfiguration
) {

    @PostMapping("/extract")
    fun extract(@RequestBody request: ExtractRequest): ExtractResponse {
        val extractorCount = coordinatorConfiguration.extractors.size
        val extractorId = Random.nextInt(0, extractorCount)

        val extractor = coordinatorConfiguration.extractors[extractorId]
        return extractorClient.extract(extractor, request)
    }
}
