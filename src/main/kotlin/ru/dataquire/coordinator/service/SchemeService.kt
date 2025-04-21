package ru.dataquire.coordinator.service

import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.DSL.constraint
import org.jooq.impl.DSL.field
import org.jooq.impl.DefaultDataType
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
import java.util.*

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
                extractScheme(connection)
            }

            recipientConnection.use { connection ->
                val dsl = DSL.using(connection)

                dsl.createDatabaseIfNotExists(catalog).execute()
                dsl.setCatalog(catalog).execute()

                tables.forEach { table -> createTable(dsl, table) }
            }
            logger.info("[MIGRATE SCHEME] Successfully migrated to {}", request.recipient)
            SchemeMigrateResponse("Migration finished", tables)
        } catch (ex: SQLException) {
            logger.error("[MIGRATE SCHEME] {}", ex.localizedMessage)
            SchemeMigrateResponse("Migration failed: ${ex.localizedMessage}", emptyList())
        }
    }

    private fun extractScheme(connection: Connection): List<DataQuireTable> {
        logger.info("[EXTRACT SCHEME] Begin extract from {}", connection.catalog)
        val tableConverter = TableConverter(connection)
        return tableConverter.getTables()
    }

    private fun createTable(dsl: DSLContext, table: DataQuireTable) {
        logger.debug("[CREATE TABLE] {}", table)
        with(dsl) {
            val fields = table.fields.map { field ->
                field(
                    field.name,
                    DefaultDataType
                        .getDataType(dsl.dialect(), field.type.name)
                        .nullable(field.type.isNullable)
                        .length(field.type.length)
                )
            }
            createTableIfNotExists(table.name).columns(fields).execute()
        }
        logger.debug("[CREATE TABLE] Table created {}", table)
    }

    private fun createConstraints(dsl: DSLContext, table: DataQuireTable) {
        with(dsl) {
            if (table.primaryKey != null) {
                val constraintPrimaryKey = constraint(
                    "${table.primaryKey.table}_pk"
                ).primaryKey(
                    table.primaryKey.field
                )
                alterTable(table.name)
                    .add(constraintPrimaryKey)
                    .execute()
            }

            table.uniqueKeys.forEach { key ->
                val constraintUniqueKey = constraint("${key.field}_uq").unique(key.field)
                alterTable(table.name)
                    .add(constraintUniqueKey)
                    .execute()
            }

            table.foreignKeys.forEach { key ->
                val constraintName = "${key.parent.table}_${key.parent.field}_fk_${UUID.randomUUID()}"
                val constraintForeignKey = constraint(constraintName)
                    .foreignKey(key.children.field)
                    .references(key.parent.table, key.parent.field)
                alterTable(table.name)
                    .add(constraintForeignKey)
                    .execute()
            }
        }
    }

    private fun DataQuireDataSource.getConnection(): Connection = DriverManager.getConnection(url, username, password)

    private fun Connection.isConnected(): Boolean = isClosed.not()
}
