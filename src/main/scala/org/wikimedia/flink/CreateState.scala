package org.wikimedia.flink

import java.lang

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.state.api.{OperatorTransformation, Savepoint}
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction

/**
 * Creates a state of N keys mapping to a random long value
 */
object CreateState {
  def main(args: Array[String]): Unit = {
    implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)
    val savepoint = Savepoint.create(new EmbeddedRocksDBStateBackend(true), 1024)
    val dataSet: DataSource[Tuple2[String, lang.Long]] = env.getJavaEnv.fromCollection(new SequenceIterator(params.getInt("size")),
      createTypeInformation[Tuple2[String, lang.Long]])
    val operator = OperatorTransformation.bootstrapWith(dataSet)
      .keyBy(new KeySelector[Tuple2[String, lang.Long], String] {
        override def getKey(in: Tuple2[String, lang.Long]): String = in.f0
      })
      .transform(new SimpleState())

    savepoint
      .withOperator(StateDefinition.STATE_OP_UUID, operator)
      .write(params.get("savepoint"))
    env.execute()
  }
}

class SimpleState extends KeyedStateBootstrapFunction[String, Tuple2[String, lang.Long]] {
  var state: ValueState[lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    state = StateDefinition.openState(getRuntimeContext)
  }

  override def processElement(in: Tuple2[String, lang.Long],
                              context: KeyedStateBootstrapFunction[String,
                              Tuple2[String, lang.Long]]#Context): Unit = {
    state.update(in.f1)
  }
}
