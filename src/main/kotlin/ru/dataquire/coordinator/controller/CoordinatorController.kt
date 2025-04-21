package ru.dataquire.coordinator.controller

import jakarta.servlet.http.HttpServletRequest
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import ru.dataquire.coordinator.dto.request.MigrateRequest
import ru.dataquire.coordinator.service.CoordinatorService

@RestController
@RequestMapping("/v1/api/coordinator")
class CoordinatorController(
    private val coordinatorService: CoordinatorService
) {

    @PostMapping("/migrate")
    fun migrateAndExtract(
        @RequestBody request: MigrateRequest
    ) = coordinatorService.migrate(request)

    @PostMapping("/registration")
    fun registrationExtractor(request: HttpServletRequest) = coordinatorService.registration(request)
}