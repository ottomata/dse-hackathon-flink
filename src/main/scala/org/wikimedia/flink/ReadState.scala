package org.wikimedia.flink

import java.lang

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.state.api.Savepoint
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

/**
 * Reads all key values pair using the state-processor-api
 */
object ReadState {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)
    val savepoint = Savepoint.load(env, params.get("savepoint"), new EmbeddedRocksDBStateBackend(true))
    val count = savepoint.readKeyedState(StateDefinition.STATE_OP_UUID, new StateReader(), createTypeInformation[String], createTypeInformation[Tuple2[String, lang.Long]])
      .count()
    LOG.info("Got {} entries", count)
  }
}

class StateReader extends KeyedStateReaderFunction[String, Tuple2[String, lang.Long]] {
  var state: ValueState[lang.Long] = _

  override def readKey(k: String,
                       context: KeyedStateReaderFunction.Context,
                       collector: Collector[Tuple2[String, lang.Long]]): Unit = {
    collector.collect(new Tuple2(k, state.value()))
  }

  override def open(parameters: Configuration): Unit = {
    state = StateDefinition.openState(getRuntimeContext)
  }
}
