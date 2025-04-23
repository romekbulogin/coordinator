package ru.dataquire.coordinator.controller

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import ru.dataquire.coordinator.dto.datasource.DataQuireDataSource
import ru.dataquire.coordinator.dto.request.MigrateRequest
import ru.dataquire.coordinator.dto.response.SchemeMigrateResponse
import ru.dataquire.coordinator.service.SchemeService

@RestController
@RequestMapping("/v1/api/scheme")
class SchemeController(
    private val schemeService: SchemeService
) {

    @PostMapping("/migrate")
    fun migrate(
        @RequestBody request: MigrateRequest
    ): SchemeMigrateResponse = schemeService.migrate(request)

    @GetMapping("/tables")
    fun getTables(
        @RequestBody request: DataQuireDataSource
    ): List<String> = schemeService.getTables(request)
}
