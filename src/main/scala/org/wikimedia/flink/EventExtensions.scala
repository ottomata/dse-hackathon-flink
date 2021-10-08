package org.wikimedia.flink

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.table.api.{FormatDescriptor, Schema, Table, TableDescriptor, TableEnvironment}
import org.wikimedia.eventutilities.core.event.EventStream

import collection.JavaConverters._

object EventExtensions {

    val KAFKA_CONNECTOR_OPTION_DEFAULTS = Map(
        "properties.bootstrap.servers" -> "kafka-jumbo1001.eqiad.wmnet:9092",
        "scan.startup.mode" ->  "latest-offset"
    )



    implicit class EventStreamExtensions(es: EventStream) {

        /**
         * Returns a Flink Schema.Builder for this EventStream in Kafka.
         * @param withKafkaWatermark If true, a kafka_timestamp virtual field will be added
         *                           to the schema and used as the event time watermark.
         * @return
         */
        def flinkSchemaBuilder(withKafkaWatermark: Boolean = true): Schema.Builder = {
            val builder = new JsonSchemaConverter().getSchemaBuilder(es.schema().asInstanceOf[ObjectNode])
            if (withKafkaWatermark) {
                builder.withKafkaTimestamp()
            }
            builder
        }

        /**
         * Returns a Flink TableDescriptor for this EventStream, using the kafka_timestamp as the
         * event time watermark.
         * @param connectorOptions Additional options to pass to the Kafka connector.
         * @return
         */
        def flinkKafkaTableDescriptorBuilder(
             connectorOptions: Map[String, String] = Map()
         ): TableDescriptor.Builder = {
            val td = TableDescriptor.forConnector("kafka")
                .option("topic", es.topics().asScala.mkString(";"))
                .schema(flinkSchemaBuilder(true).build())
                .format("json")

            // apply provided options
            KAFKA_CONNECTOR_OPTION_DEFAULTS.foreach({
                case (k,v) => td.option(k, v)
            })

            // apply provided options.
            connectorOptions.foreach({
                case (k,v) => td.option(k, v)
            })

            td
        }

        /**
         * Registers this EventStream as a temporary Flink streaming table
         * @param tableEnv
         * @param connectorOptions Additional options to pass to the Kafka connector.
         * @param tableName Name of the table, default normalized streamName.
         * @return
         */
        def flinkRegisterKafkaTable(
            tableEnv: TableEnvironment,
            connectorOptions: Map[String, String] = Map(),
            tableName: String = null
        ): Table = {
            val tableBuilder = flinkKafkaTableDescriptorBuilder(connectorOptions)

            val tableNameFinal = if (tableName == null) {
                JsonSchemaConverter.sanitizeFieldName(es.streamName())
            } else {
                tableName
            }

            tableEnv.createTemporaryTable(
                tableNameFinal,
                tableBuilder.build()
            )

            tableEnv.from(tableNameFinal)
        }
    }


    // TODO: this is silly as an implicit class.
    implicit class SchemaBuilderExtensions(builder: Schema.Builder) {

        /**
         * Adds the kafka timestamp as the event time watermark to the Schema.Builder.
         * @param columnName
         * @param asWatermark
         * @param watermarkSql
         * @return
         */
        def withKafkaTimestamp(
            columnName: String = "kafka_timestamp",
            asWatermark: Boolean = true,
            watermarkSql: String = null
        ): Schema.Builder = {
            val watermarkSqlExpr = if (watermarkSql == null)
                columnName
            else
                watermarkSql

            builder.columnByMetadata(
                "kafka_timestamp",
                "TIMESTAMP_LTZ(3) NOT NULL",
                "timestamp",
                true
            )

            if (asWatermark) {
                builder.watermark(columnName, watermarkSqlExpr)
            }

            builder
        }



    }
}

