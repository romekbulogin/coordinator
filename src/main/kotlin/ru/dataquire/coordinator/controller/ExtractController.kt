package ru.dataquire.coordinator.controller

import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import ru.dataquire.coordinator.configuration.CoordinatorConfiguration
import ru.dataquire.coordinator.dto.request.ExtractRequest
import ru.dataquire.coordinator.service.ExtractorClient
import kotlin.random.Random

@RestController
@RequestMapping("/v1/api/coordinator/extractor")
class ExtractController(
    private val extractorClient: ExtractorClient,
    private val coordinatorConfiguration: CoordinatorConfiguration
) {

    @PostMapping("/extract")
    fun extract(@RequestBody request: ExtractRequest) {
        val extractorCount = coordinatorConfiguration.extractors.size
        val extractorId = Random(1337).nextInt(0, extractorCount)

        val extractor = coordinatorConfiguration.extractors[extractorId]

        extractorClient.extract(extractor, request)
    }
}