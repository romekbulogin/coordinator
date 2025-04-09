package ru.dataquire.coordinator.dto.keys

data class DataQuireUniqueKey(
    override val table: String,
    override val field: String,
) : DataQuireAbstractKey(table, field)
