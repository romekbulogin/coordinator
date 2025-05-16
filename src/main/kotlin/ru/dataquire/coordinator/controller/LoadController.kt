package ru.dataquire.coordinator.controller

import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import ru.dataquire.coordinator.client.LoaderClient
import ru.dataquire.coordinator.configuration.CoordinatorConfiguration
import ru.dataquire.coordinator.dto.request.ExtractRequest
import ru.dataquire.coordinator.dto.request.LoadRequest
import ru.dataquire.coordinator.dto.response.LoadResponse
import kotlin.random.Random

@RestController
@RequestMapping("/v1/api/coordinator/loader")
class LoadController(
    private val loaderClient: LoaderClient,
    private val coordinatorConfiguration: CoordinatorConfiguration
) {

    @PostMapping("/load")
    fun load(@RequestBody request: LoadRequest): LoadResponse {
        val loaderCount = coordinatorConfiguration.loaders.size
        val loaderId = Random.nextInt(0, loaderCount)

        val loader = coordinatorConfiguration.loaders[loaderId]
        return loaderClient.load(loader, request)
    }
}