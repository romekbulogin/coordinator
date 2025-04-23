package ru.dataquire.coordinator.service

import org.jooq.impl.DSL.using
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.dataquire.coordinator.dto.DataQuireTable
import ru.dataquire.coordinator.dto.datasource.DataQuireDataSource
import ru.dataquire.coordinator.dto.request.MigrateRequest
import ru.dataquire.coordinator.dto.response.SchemeMigrateResponse
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

@Service
class SchemeService {
    private val logger: Logger = LoggerFactory.getLogger(SchemeService::class.java)

    fun migrate(request: MigrateRequest): SchemeMigrateResponse {
        return try {
            logger.info("[MIGRATE SCHEME] Migration started from {}", request.origin)
            val originConnection: Connection = request.origin.getConnection()
            val recipientConnection: Connection = request.recipient.getConnection()

            logger.debug("[MIGRATE SCHEME] Check connection to {}", request.origin)
            check(originConnection.isConnected()) { "Origin is not connected" }

            logger.debug("[MIGRATE SCHEME] Check connection to {}", request.recipient)
            check(recipientConnection.isConnected()) { "Recipient is not connected" }

            val catalog = originConnection.catalog

            val tables: List<DataQuireTable> = originConnection.use { connection ->
                val tableConverter = TableConverter(connection)
                if (request.tables.isNotEmpty()) {
                    logger.debug("[CONVERTER] Converting tables {} in {}", request.tables, connection.catalog)
                    tableConverter.getTables(request.tables)
                } else {
                    logger.debug("[CONVERTER] Converting tables in {}", connection.catalog)
                    tableConverter.getTables()
                }
            }

            recipientConnection.use { connection ->
                val dsl = using(connection)
                val tableConverter = TableConverter(connection)

                dsl.createDatabaseIfNotExists(catalog).execute()
                tables.forEach { table -> tableConverter.createTable(table) }
            }
            logger.info("[MIGRATE SCHEME] Successfully migrated to {}", request.recipient)
            SchemeMigrateResponse("Migration finished", tables)
        } catch (ex: SQLException) {
            logger.error("[MIGRATE SCHEME] {}", ex.localizedMessage)
            SchemeMigrateResponse("Migration failed: ${ex.localizedMessage}", emptyList())
        }
    }

    fun getTables(dataSource: DataQuireDataSource): List<String> = dataSource
        .getConnection()
        .use { connection -> TableConverter(connection).getTablesName() }

    private fun DataQuireDataSource.getConnection(): Connection = DriverManager.getConnection(url, username, password)

    private fun Connection.isConnected(): Boolean = isClosed.not()
}
