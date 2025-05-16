package ru.dataquire.coordinator

import org.jooq.impl.BlobBinding
import org.jooq.impl.DSL
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import java.sql.DriverManager

@SpringBootTest
class TableManagerTest {
    private val postgresConnection = DriverManager.getConnection(
        "jdbc:postgresql://localhost:5432/family_tree",
        "postgres",
        "1337"
    )
    private val mySqlConnection = DriverManager.getConnection(
        "jdbc:mysql://localhost:3306/family_tree",
        "root",
        "1337"
    )


    @Test
    fun getTablesTest() {
        postgresConnection.use { connection ->
            val dsl = DSL.using(connection)
            println("SCHEMA: ${connection.schema}")
            println("CATALOG: ${connection.catalog}")

            val tables = if (dsl.meta().getSchemas(connection.schema).isEmpty()) {
                dsl.meta().tables
            } else {
                dsl.meta().getSchemas(connection.schema).first().tables
            }
            tables.forEach { table ->
                val fields = table.fields()
                val size = fields.map { it.dataType.precision() }
                println(size)
            }
        }
    }

    @Test
    fun findTables() {
        val tablesName = listOf("owner", "person")

        postgresConnection.use { connection ->
            val dsl = DSL.using(connection)

            val tables = if (dsl.meta().getSchemas(connection.schema).isEmpty()) {
                dsl.meta().tables
            } else {
                dsl.meta().getSchemas(connection.schema).first().tables
            }

            tables
                .filter { it.name in tablesName }
                .forEach { table -> println(table) }
        }
    }
}