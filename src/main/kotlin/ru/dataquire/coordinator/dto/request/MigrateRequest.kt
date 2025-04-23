package ru.dataquire.coordinator.dto.request

import ru.dataquire.coordinator.dto.datasource.DataQuireDataSource

data class MigrateRequest(
    val origin: DataQuireDataSource,
    val recipient: DataQuireDataSource,
    val tables: List<String> = listOf(),
)
