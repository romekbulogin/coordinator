package ru.dataquire.coordinator.dto.request

import ru.dataquire.coordinator.dto.datasource.DataQuireDataSource

data class LoadRequest(
    val table: String,
    val dataSource: DataQuireDataSource,
    val origin: DataQuireDataSource,
)