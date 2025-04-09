package ru.dataquire.coordinator.dto.keys.foreignkey

data class DataQuireForeignKey(
    val parent: DataQuireParentFieldForeignKey,
    val children: DataQuireChildrenFieldForeignKey
)
