package ru.dataquire.coordinator.dto.request

import ru.dataquire.coordinator.dto.datasource.DataQuireDataSource

data class ExtractRequest(
    val table: String,
    val dataSource: DataQuireDataSource
)
