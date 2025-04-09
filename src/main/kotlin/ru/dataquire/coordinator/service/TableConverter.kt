package ru.dataquire.coordinator.service

import org.jooq.DSLContext
import org.jooq.ForeignKey
import org.jooq.Table
import org.jooq.UniqueKey
import org.jooq.impl.DSL.using
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

class TableConverter(
    private val connection: Connection
) {
    private val logger: Logger = LoggerFactory.getLogger(TableConverter::class.java)

    private val dsl: DSLContext = using(connection)
    private val schema = dsl.meta().getSchemas(connection.schema)[0]

    fun getTables(): List<DataQuireTable> {
        logger.debug("[TABLE] Get \"${connection.catalog}\" tables")
        val tables: List<DataQuireTable> = schema.tables.map { table ->
            DataQuireTable(
                name = table.name,
                fields = table.fields().map { field ->
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
                primaryKey = table.toPrimaryKey(),
                uniqueKeys = table.uniqueKeys.toUniqueKeys(),
                foreignKeys = table.references.toForeignKeys()
            )
        }
        return tables
    }

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
