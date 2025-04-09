package ru.dataquire.coordinator.dto

data class DataQuireType(
    val name: String,
    val length: Int,
    val sqlType: Int,
    val isNullable: Boolean
)
