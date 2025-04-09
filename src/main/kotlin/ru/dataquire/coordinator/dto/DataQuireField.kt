package ru.dataquire.coordinator.dto

data class DataQuireField(
    val name: String,
    val type: DataQuireType,
    val defaultValue: Any?,
)
