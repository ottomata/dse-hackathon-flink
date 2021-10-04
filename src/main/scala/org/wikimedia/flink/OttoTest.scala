package org.wikimedia.flink

import java.lang

import scala.collection.JavaConverters.asScalaIteratorConverter

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.scala.{createTypeInformation, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import org.wikimedia.eventutilities.core.event.WikimediaDefaults

/**
 * Re-read all keys in reverse order
 */
object OttoTest {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {

  }
}
