//package org.wikimedia.flink
//
//import com.fasterxml.jackson.databind.node.ObjectNode
//import org.apache.flink.table.api.{EnvironmentSettings, FormatDescriptor, Schema, Table, TableDescriptor, TableEnvironment}
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.table.catalog.hive.HiveCatalog
//import org.apache.hadoop.security.UserGroupInformation
//import org.wikimedia.flink.EventExtensions._
//
//import java.security.PrivilegedExceptionAction
//
///**
// * Re-read all keys in reverse order
// */
//object OttoTest {
//
//    def main(args: Array[String]): Unit = {
//
////        val tableEnv = getTableEnv()
////        val pageLinksChange = createPageLinksChangeTable(tableEnv)
////
////        val result = tableEnv.sqlQuery(
////            "SELECT database, count(*) FROM page_links_change GROUP BY database ORDER BY count(*) DESC"
////        )
////        tableEnv.createTemporaryTable(
////            "output",
////            TableDescriptor.forConnector("print")
//////                .format(FormatDescriptor.forFormat("json").build())
////                .schema(Schema.newBuilder().fromResolvedSchema(result.getResolvedSchema).build())
////                .build()
////        )
////
////        result.executeInsert("output")
//
//        doStuff2()
//  }
//
//
//
//    def doStuff2() = {
//        import org.wikimedia.eventutilities.core.event.WikimediaDefaults
//        import org.wikimedia.flink.EventExtensions._
//
//        val tableEnv = getStreamingTableEnv()
//
//        val es = WikimediaDefaults.EVENT_STREAM_FACTORY.createEventStream("mediawiki.page-links-change")
//
//        es.flinkRegisterKafkaTable(tableEnv)
//
//
//        val result = tableEnv.sqlQuery(
//            """
//              |SELECT TUMBLE_START(kafka_timestamp, INTERVAL '1' MINUTE), database, COUNT(database)
//              |FROM mediawiki_page_links_change
//              |GROUP BY TUMBLE(kafka_timestamp, INTERVAL '1' MINUTE), database
//              |""".stripMargin
//        )
//
//        tableEnv.createTemporaryTable(
//            "output",
//            TableDescriptor.forConnector("print")
//                .schema(Schema.newBuilder().fromResolvedSchema(result.getResolvedSchema).build())
//                .build()
//        )
//
//        result.executeInsert("output")
//
//    }
//
//
//    def getTableEnv(): TableEnvironment = {
//        val settings = EnvironmentSettings.newInstance().inBatchMode().build()
//        TableEnvironment.create(settings)
//    }
//
//    def getStreamingTableEnv(): TableEnvironment = {
//        val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
//        TableEnvironment.create(settings)
//    }
//
//
//    def createHiveCatalogTable()= {
//        val tableEnv = getStreamingTableEnv()
//        val tableEnv = StreamTableEnvironment.create(senv)
//val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
//        val tableEnv = TableEnvironment.create(settings)
//
//        import org.apache.hadoop.hive.conf.HiveConf
//        import org.apache.flink.table.catalog._
//
//        val tableEnv = StreamTableEnvironment.create(senv)
//        import org.apache.flink.table.api._
//        import org.apache.flink.table.catalog.hive.HiveCatalog
//        val catalog = new HiveCatalog("analytics_hive", "flink_test", "/etc/hive/conf")
//        tableEnv.registerCatalog("analytics_hive", catalog)
//
//
//
//
////        val USER_NAME = "user.name"
//        // Some Hive Metastore properties
//        val HIVE_METASTORE_SASL_ENABLED = "hive.metastore.sasl.enabled"
//        val HIVE_METASTORE_KERBEROS_PRINCIPAL = "hive.metastore.kerberos.principal"
//        val HIVE_METASTORE_KERBEROS_KEYTAB = "hive.metastore.kerberos.keytab.file"
//        val HIVE_METASTORE_LOCAL = "hive.metastore.local"
////        val HADOOP_RPC_PROTECTION = "hadoop.rpc.protection"
//
//
//        //
//        val hiveConf = catalog.getHiveConf()
//
//        val principal = "analytics-privatedata/stat1006.eqiad.wmnet@WIKIMEDIA"
//        val server = "thrift://analytics-hive.eqiad.wmnet:9083"
//        val keytab = "/etc/security/keytabs/analytics-privatedata/analytics-privatedata.keytab"
//        hiveConf.set(HIVE_METASTORE_SASL_ENABLED, "true")
//        hiveConf.set(HIVE_METASTORE_KERBEROS_PRINCIPAL, principal)
//        hiveConf.set(HIVE_METASTORE_LOCAL, "false")
//        hiveConf.set(HIVE_METASTORE_KERBEROS_KEYTAB, keytab)
//        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, server)
//
//
//        val catalog2 = new HiveCatalog("analytics_hive", "otto_flink_test", hiveConf, "2.3.6")
//
//        tableEnv.registerCatalog("analytics_hive", catalog2)
//
//
//
//    }
//
//    // https://blog.csdn.net/weibokong789/article/details/106427481
//    def createKerbHiveCatalog() = {
//
//        import org.apache.flink.table.catalog.hive.HiveCatalog
//        import org.apache.hadoop.security.UserGroupInformation
//        import java.security.PrivilegedExceptionAction
//
//        val principal = "analytics-privatedata/stat1006.eqiad.wmnet@WIKIMEDIA"
//        val server = "thrift://analytics-hive.eqiad.wmnet:9083"
//
//        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
//        System.setProperty("java.security.auth.useSubectCredsOnly", "false")
//        System.setProperty("sun.security.krb5.debug", "true")
//        UserGroupInformation.loginUserFromKeytab(principal, server)
//        System.out.println(UserGroupInformation.getCurrentUser())
//
//        val hiveCatalog = UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction[HiveCatalog]() {
//            @Override
//            def run(): HiveCatalog = {
//                new HiveCatalog("analytics_hive", "otto_flink_test", "/etc/hive/conf")
//            }
//        })
//
//        val tableEnv = StreamTableEnvironment.create(senv)
//
//        tableEnv.registerCatalog("analytics_hive", hiveCatalog)
//
//    }
//
//    def createFilePageLinksChangeTable(tableEnv: TableEnvironment): Table = {
//        val es = WikimediaExternalDefaults.EVENT_STREAM_FACTORY.createEventStream("mediawiki.page-links-change")
//
//        val flinkSchema = new JsonSchemaConverter().getSchemaBuilder(es.schema().asInstanceOf[ObjectNode]).build();
//
//        tableEnv.createTemporaryTable(
//            "page_links_change",
//            TableDescriptor.forConnector("filesystem")
//                .schema(flinkSchema)
//                .option("path", "/tmp/page-links.change.json")
//                .format(FormatDescriptor.forFormat("json").build())
//                .build()
//        )
//        tableEnv.from("page_links_change")
//    }
//
//    def doStuff(): Unit = {
//        import org.wikimedia.eventutilities.core.event.WikimediaDefaults
//        import org.wikimedia.flink.JsonSchemaConverter
//        import com.fasterxml.jackson.databind.node.ObjectNode
//
//        val tableEnv = getStreamingTableEnv()
//
//        val es = WikimediaDefaults.EVENT_STREAM_FACTORY.createEventStream("mediawiki.page-links-change")
//
//        val flinkSchemaBuilder = new JsonSchemaConverter().getSchemaBuilder(es.schema().asInstanceOf[ObjectNode])
//
//        flinkSchemaBuilder.columnByMetadata(
//            "kafka_timestamp",
//            "TIMESTAMP_LTZ(3) NOT NULL",
//            "timestamp",
//            true
//        )
//        flinkSchemaBuilder.watermark("kafka_timestamp", "kafka_timestamp")
//
//        val flinkSchema = flinkSchemaBuilder.build()
//
//        tableEnv.createTemporaryTable(
//            "page_links_change_stream",
//            TableDescriptor.forConnector("kafka")
//                .option("topic", "eqiad.mediawiki.page-links-change")
//                .option("properties.bootstrap.servers", "kafka-jumbo1001.eqiad.wmnet:9092")
//                .option("properties.group.id", "otto-test-flink")
//                .option("scan.startup.mode", "latest-offset")
//                .schema(flinkSchema)
//                .format(FormatDescriptor.forFormat("json").build())
//                .build()
//        )
//
//        val t = tableEnv.from("page_links_change_stream")
//
//
//        val result = tableEnv.sqlQuery(
//            """
//              |SELECT TUMBLE_START(kafka_timestamp, INTERVAL '1' MINUTE), database, COUNT(DISTINCT database)
//              |FROM page_links_change_stream
//              |GROUP BY TUMBLE(kafka_timestamp, INTERVAL '1' MINUTE), database
//              |""".stripMargin
//        )
//
//        tableEnv.createTemporaryTable(
//            "output",
//            TableDescriptor.forConnector("print")
//                //                .format(FormatDescriptor.forFormat("json").build())
//                .schema(Schema.newBuilder().fromResolvedSchema(result.getResolvedSchema).build())
//                .build()
//        )
//
//        result.executeInsert("output")
//    }
//}
//
//
//
