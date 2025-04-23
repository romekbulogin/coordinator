package ru.dataquire.coordinator.service

import org.jooq.DSLContext
import org.jooq.ForeignKey
import org.jooq.Table
import org.jooq.UniqueKey
import org.jooq.impl.DSL.constraint
import org.jooq.impl.DSL.field
import org.jooq.impl.DSL.using
import org.jooq.impl.DefaultDataType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.dataquire.coordinator.dto.DataQuireField
import ru.dataquire.coordinator.dto.DataQuireTable
import ru.dataquire.coordinator.dto.DataQuireType
import ru.dataquire.coordinator.dto.keys.DataQuirePrimaryKey
import ru.dataquire.coordinator.dto.keys.DataQuireUniqueKey
import ru.dataquire.coordinator.dto.keys.foreignkey.DataQuireChildrenFieldForeignKey
import ru.dataquire.coordinator.dto.keys.foreignkey.DataQuireForeignKey
import ru.dataquire.coordinator.dto.keys.foreignkey.DataQuireParentFieldForeignKey
import java.sql.Connection
import java.util.*

class TableConverter(
    private val connection: Connection
) {
    private val logger: Logger = LoggerFactory.getLogger(TableConverter::class.java)

    private val dsl: DSLContext = using(connection)
    private val meta = dsl.meta()
    private val catalog = meta.getCatalog(connection.catalog)
    private val schema = meta.getSchemas(connection.schema).firstOrNull()

    fun getTables(): List<DataQuireTable> {
        logger.debug("[TABLES] Get \"{}\" tables", connection.catalog)
        val tables = schema?.tables ?: meta.tables
        return tables.map { table -> table.toTable() }
    }

    fun getTables(tables: List<String>): List<DataQuireTable> {
        require(tables.isNotEmpty()) { "Tables can not be empty" }
        logger.debug("[TABLES IN LIST] Get \"{}\" tables={}", connection.catalog, tables)

        val tablesInSchema = schema?.tables ?: meta.tables
        val dataQuireTables: List<DataQuireTable> = tablesInSchema
            .filter { table -> table.name in tables }
            .map { table -> table.toTable() }
        return dataQuireTables
    }

    fun createTable(table: DataQuireTable) {
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

    fun createConstraints(table: DataQuireTable) {
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

    fun getTablesName(): List<String> {
        val tables = schema?.tables ?: meta.tables
        return tables.map { it.name }
    }

    private fun Table<*>.toTable(): DataQuireTable = DataQuireTable(
        name = name,
        fields = fields().map { field ->
            DataQuireField(
                name = field.name,
                type = DataQuireType(
                    name = field.dataType.castTypeName,
                    length = field.dataType.length(),
                    sqlType = field.dataType.sqlType,
                    isNullable = field.dataType.nullable()
                ),
                defaultValue = field.dataType.defaultValue().toString(),
            )
        },
        primaryKey = toPrimaryKey(),
        uniqueKeys = uniqueKeys.toUniqueKeys(),
        foreignKeys = references.toForeignKeys()
    )

    private fun Table<*>.toPrimaryKey(): DataQuirePrimaryKey? {
        val primaryKey = primaryKey
        return if (primaryKey != null) {
            DataQuirePrimaryKey(
                table = name,
                field = primaryKey.fields[0]?.name,
            )
        } else {
            null
        }
    }

    private fun List<UniqueKey<*>>.toUniqueKeys() = flatMap { uniqueKey ->
        uniqueKey.fields.map { field ->
            DataQuireUniqueKey(
                table = uniqueKey.table.name,
                field = field.name,
            )
        }
    }

    private fun List<ForeignKey<*, *>>.toForeignKeys(): List<DataQuireForeignKey> {
        val parents = map { reference -> reference.keyFields }.flatMap { parent ->
            parent.mapIndexed { index, tableField ->
                DataQuireParentFieldForeignKey(
                    table = tableField.table!!.name,
                    field = parent[index].name
                )
            }
        }
        val children = map { reference -> reference.fields }.flatMap { parent ->
            parent.mapIndexed { index, tableField ->
                DataQuireChildrenFieldForeignKey(
                    table = tableField.table!!.name,
                    field = parent[index].name
                )
            }
        }

        val foreignKeys = parents.zip(children).map {
            DataQuireForeignKey(
                parent = it.first,
                children = it.second
            )
        }
        return foreignKeys
    }
}
