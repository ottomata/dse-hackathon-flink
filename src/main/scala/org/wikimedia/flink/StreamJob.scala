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

/**
 * Re-read all keys in reverse order
 */
object StreamJob {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true))
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val size = params.getInt("size")
    val source = env.fromCollection(new SequenceIterator(size = size, reversed = true).asScala)

    source
      .keyBy(_.f0)
      .process(new ReadStateFunction())
      .uid(StateDefinition.STATE_OP_UUID)
      .addSink(new DiscardingSink[String])

    env.execute()
  }
}

class ReadStateFunction extends KeyedProcessFunction[String, Tuple2[String, lang.Long], String] {
  var state: ValueState[lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    state = StateDefinition.openState(getRuntimeContext)
  }

  override def processElement(i: Tuple2[String, lang.Long],
                              context: KeyedProcessFunction[String, Tuple2[String, lang.Long], String]#Context,
                              collector: Collector[String]): Unit = {
    val storedValue = state.value()
    if (storedValue != null) {
      collector.collect("found")
    } else {
      throw new IllegalArgumentException("all keys should be found " + context.getCurrentKey)
    }
  }
}
