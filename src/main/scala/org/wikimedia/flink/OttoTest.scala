package org.wikimedia.flink

import com.fasterxml.jackson.databind.node.ObjectNode

import java.lang
import scala.collection.JavaConverters.asScalaIteratorConverter
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import org.wikimedia.eventutilities.core.event.WikimediaExternalDefaults
import org.apache.flink.formats.json._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.types.Row
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.table.api.{EnvironmentSettings, FormatDescriptor, TableDescriptor, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types._
import org.wikimedia.eventutilities.core.event.WikimediaExternalDefaults;

/**
 * Re-read all keys in reverse order
 */
object OttoTest {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)

//    val es = WikimediaExternalDefaults.EVENT_STREAM_FACTORY.createEventStream("mediawiki.page-links-change")
//    val deserializationSchema = new       JsonNamedRowDeserializationSchema.Builder(es.schema().toString()).build

    def main(args: Array[String]): Unit = {

//    val es = WikimediaExternalDefaults.EVENT_STREAM_FACTORY.createEventStream("mediawiki.page-links-change")
//    val flinkTypeInfo = JsonRowSchemaConverter.convert(es.schema().toString())
//    val deserialisationSchema = new JsonNamedRowDeserializationSchema.Builder(es.schema().toString()).build
//

////        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//        val env = ExecutionEnvironment.createLocalEnvironment()
////        val tableEnv = TableEnvironment.create(env.getConfiguration())
//
//
//        val settings = EnvironmentSettings.fromConfiguration(env.getConfiguration)
//
//        val tableEnv = TableEnvironment.create(settings)


        val settings = EnvironmentSettings
            .newInstance()
//            .inStreamingMode()
            .inBatchMode()
            .build()
        val tableEnv = TableEnvironment.create(settings)

        val es = WikimediaExternalDefaults.EVENT_STREAM_FACTORY.createEventStream("mediawiki.page-links-change");

        //    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
//        val params: ParameterTool = ParameterTool.fromArgs(args)
//        val size = 20; //params.getInt("size")
//        env.fromCollection(new EventGenerator(size = size).asScala)
//            .map(v => deserialize(v))
//            .print()



        val flinkSchema = new JsonSchemaConverter().getSchemaBuilder(es.schema().asInstanceOf[ObjectNode]).build();

        tableEnv.createTemporaryTable(
            "page_links_change",
            TableDescriptor.forConnector("filesystem")
                .schema(flinkSchema)
                .option("path", "/Users/otto/Projects/wm/analytics/flink-dse-hackathon-2021/page-links.change.json")
                .format(FormatDescriptor.forFormat("json").build())
                .build()
        )

//        val result = tableEnv.sqlQuery(
//            "SELECT database, count(*) FROM page_links_change GROUP BY database ORDER BY count(*) DESC"
//        );
        val table = tableEnv.from("page_links_change")

        println(table.explain())

//        tableEnv.execute()
  }

//    def deserialize(v: Array[Byte]): Row = {
//        deserializationSchema.deserialize(v)
//    }
}






//import org.apache.flink.annotation.PublicEvolving
//import org.apache.flink.streaming.api.functions.sink.SinkFunction
//
//@PublicEvolving
//@SuppressWarnings(Array("unused"))
//@SerialVersionUID(1L)
//class PrintSink extends SinkFunction[Array[Byte]] {
//  override def invoke(value: Array[Byte], context: SinkFunction.Context): Unit = {
//    println(value.mkString(""))
//  }
//}