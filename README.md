# DSE Hackathon Flink Data Integration experiments

## Get a Flink Table Schema from an Event Platform stream

```scala
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.table.api.{EnvironmentSettings, FormatDescriptor, Schema, Table, TableDescriptor, TableEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.wikimedia.flink.EventExtensions._


import org.wikimedia.eventutilities.core.event.WikimediaDefaults

// Get a streaming TableEnvironment
val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
TableEnvironment.create(settings)


// A declared Event Platform stream.
val streamName = "mediawiki.page-links-change"

// get an EventStream object using wikimedia event-utillities.
val eventStream = WikimediaDefaults.EVENT_STREAM_FACTORY.createEventStream(streamName)

// Register a streaming page links change table.
val pageLinksChangeStreamTable = eventStream.flinkRegisterKafkaTable(tableEnv)

// Query it however you like
val result = tableEnv.sqlQuery(
   """
     |SELECT TUMBLE_START(kafka_timestamp, INTERVAL '1' MINUTE), database, COUNT(DISTINCT database)
     |FROM mediawiki_page_links_change
     |GROUP BY TUMBLE(kafka_timestamp, INTERVAL '1' MINUTE), database
     |""".stripMargin
)

// print the result, or insert it into a Sink connector table.
result.execute().print()
```

## Or just get the Flink schema builder and use it to create whatever kind of table you like.
```scala
// Get a flink Table schema matching the event stream's latest
// JSONSchema, with a kafka_timestamp as the watermark event time.
val flinkSchemaBuilder = eventStream.flinkRegisterKafkaTable(tableEnv).build()

// Create a Flink streaming Table from the local filesystem..
tableEnv.createTemporaryTable(
    "mediawiki_page_links_change",
    TableDescriptor.forConnector("filesystem")
        .schema(flinkSchema)
        .option("path", "/tmp/page-links.change.json")
        .format(FormatDescriptor.forFormat("json").build())
        .build()
)

val pageLinksChangeTable = tableEnv.from("mediawiki_page_links_change")
```


