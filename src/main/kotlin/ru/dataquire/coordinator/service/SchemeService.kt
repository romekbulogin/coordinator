package ru.dataquire.coordinator.service

import com.fasterxml.jackson.databind.ObjectMapper
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.DSL.constraint
import org.jooq.impl.DSL.field
import org.jooq.impl.DefaultDataType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.dataquire.coordinator.dto.DataQuireTable
import ru.dataquire.coordinator.dto.request.MigrateRequest
import ru.dataquire.coordinator.dto.response.MigrateResponse
import java.sql.Connection
import java.util.*

@Service
class SchemeService(
    private val mapper: ObjectMapper
) {
    private val logger: Logger = LoggerFactory.getLogger(SchemeService::class.java)

    fun migrate(request: MigrateRequest): MigrateResponse {
        logger.info("[MIGRATE] Migration started from {}", request.origin)
        val originConnection: Connection = request.origin.getConnection()
        val recipientConnection: Connection = request.recipient.getConnection()

        logger.debug("[MIGRATE] Check connection to {}", request.origin)
        check(originConnection.isConnected()) { "Origin is not connected" }

        logger.debug("[MIGRATE] Check connection to {}", request.recipient)
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
        logger.info("[MIGRATE] Successfully migrated to {}", request.recipient)
        return MigrateResponse("Migration finished")
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

    fun getContentOfTable(dsl: DSLContext, table: DataQuireTable): List<String> {
        val records = dsl.selectFrom(table.name).fetch()
        return records.map { record ->
            mapper.writeValueAsString(record.intoMap())
        }
    }

    private fun Connection.isConnected(): Boolean = isClosed.not()
}
