package ru.dataquire.coordinator.dto

import ru.dataquire.coordinator.dto.keys.DataQuirePrimaryKey
import ru.dataquire.coordinator.dto.keys.DataQuireUniqueKey
import ru.dataquire.coordinator.dto.keys.foreignkey.DataQuireForeignKey

data class DataQuireTable(
    val name: String,
    val fields: List<DataQuireField>,
    val primaryKey: DataQuirePrimaryKey?,
    val foreignKeys: List<DataQuireForeignKey>,
    val uniqueKeys: List<DataQuireUniqueKey>,
)
