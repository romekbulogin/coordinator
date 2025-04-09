package ru.dataquire.coordinator.dto.keys.foreignkey

import ru.dataquire.coordinator.dto.keys.DataQuireAbstractKey

data class DataQuireParentFieldForeignKey(
    override val table: String,
    override val field: String
) : DataQuireAbstractKey(table, field)