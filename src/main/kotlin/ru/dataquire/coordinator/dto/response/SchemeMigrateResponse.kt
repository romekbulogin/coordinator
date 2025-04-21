package ru.dataquire.coordinator.dto.response

import ru.dataquire.coordinator.dto.DataQuireTable

data class SchemeMigrateResponse(
    val status: String,
    val tables: List<DataQuireTable>
)
