package org.wikimedia.flink

import com.fasterxml.jackson.databind.node.ObjectNode

import org.wikimedia.eventutilities.core.event.WikimediaExternalDefaults
import org.apache.flink.table.api.{EnvironmentSettings, FormatDescriptor, Schema, Table, TableDescriptor, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
s

/**
 * Re-read all keys in reverse order
 */
object OttoTest {

    def main(args: Array[String]): Unit = {

        val tableEnv = getTableEnv()
        val pageLinksChange = createPageLinksChangeTable(tableEnv)

        val result = tableEnv.sqlQuery(
            "SELECT database, count(*) FROM page_links_change GROUP BY database ORDER BY count(*) DESC"
        )
        tableEnv.createTemporaryTable(
            "output",
            TableDescriptor.forConnector("print")
//                .format(FormatDescriptor.forFormat("json").build())
                .schema(Schema.newBuilder().fromResolvedSchema(result.getResolvedSchema).build())
                .build()
        )

        result.executeInsert("output")

  }



    def getTableEnv(): TableEnvironment = {
        val settings = EnvironmentSettings.newInstance().inBatchMode().build()
        TableEnvironment.create(settings)
    }

    def createPageLinksChangeTable(tableEnv: TableEnvironment): Table = {
        val es = WikimediaExternalDefaults.EVENT_STREAM_FACTORY.createEventStream("mediawiki.page-links-change")

        val flinkSchema = new JsonSchemaConverter().getSchemaBuilder(es.schema().asInstanceOf[ObjectNode]).build();

        tableEnv.createTemporaryTable(
            "page_links_change",
            TableDescriptor.forConnector("filesystem")
                .schema(flinkSchema)
                .option("path", "/Users/otto/Projects/wm/analytics/flink-dse-hackathon-2021/page-links.change.json")
                .format(FormatDescriptor.forFormat("json").build())
                .build()
        )
        tableEnv.from("page_links_change")
    }
}



