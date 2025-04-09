package ru.dataquire.coordinator.dto.datasource

import java.sql.Connection
import java.sql.DriverManager

class DataQuireDataSource(
    val url: String,
    val username: String,
    val password: String
) {
    fun getConnection(): Connection = DriverManager.getConnection(url, username, password)
}
