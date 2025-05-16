package ru.dataquire.coordinator.dto.response

data class LoadResponse(
    val isBegin: Boolean,
    val topicName: String,
    val groupId: String
)