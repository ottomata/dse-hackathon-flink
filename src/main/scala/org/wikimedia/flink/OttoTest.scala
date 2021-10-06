package org.wikimedia.flink

import com.fasterxml.jackson.databind.node.ObjectNode

import org.wikimedia.eventutilities.core.event.WikimediaDefaults
import org.apache.flink.table.api.{EnvironmentSettings, FormatDescriptor, Schema, Table, TableDescriptor, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


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
        val es = WikimediaDefaults.EVENT_STREAM_FACTORY.createEventStream("mediawiki.page-links-change")

        val flinkSchema = new JsonSchemaConverter().getSchemaBuilder(es.schema().asInstanceOf[ObjectNode]).build();

        tableEnv.createTemporaryTable(
            "page_links_change",
            TableDescriptor.forConnector("filesystem")
                .schema(flinkSchema)
                .option("path", "/tmp/page-links.change.json")
                .format(FormatDescriptor.forFormat("json").build())
                .build()
        )
        tableEnv.from("page_links_change")
    }

    def doStuff(tableEnv: StreamTableEnvironment): Unit = {
        val es = WikimediaDefaults.EVENT_STREAM_FACTORY.createEventStream("mediawiki.page-links-change")

        val flinkSchemaBuilder = new JsonSchemaConverter().getSchemaBuilder(es.schema().asInstanceOf[ObjectNode])

        flinkSchemaBuilder.columnByMetadata(
            "kafka_timestamp",
            "TIMESTAMP_LTZ(3) NOT NULL",
            "timestamp",
            true
        )
        flinkSchemaBuilder.watermark("kafka_timestamp", "kafka_timestamp")

        val flinkSchema = flinkSchemaBuilder.build()

        tableEnv.createTemporaryTable(
            "page_links_change_stream",
            TableDescriptor.forConnector("kafka")
                .option("topic", "eqiad.mediawiki.page-links-change")
                .option("properties.bootstrap.servers", "kafka-jumbo1001.eqiad.wmnet:9092")
                .option("properties.group.id", "otto-test-flink")
                .option("scan.startup.mode", "latest-offset")
                .schema(flinkSchema)
                .format(FormatDescriptor.forFormat("json").build())
                .build()
        )

        val t = tableEnv.from("page_links_change_stream")


        val result = tableEnv.sqlQuery(
            """
              |SELECT TUMBLE_START(kafka_timestamp, INTERVAL '1' MINUTE), database, COUNT(DISTINCT database)
              |FROM page_links_change_stream
              |GROUP BY TUMBLE(kafka_timestamp, INTERVAL '1' MINUTE), database
              |""".stripMargin
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
}



