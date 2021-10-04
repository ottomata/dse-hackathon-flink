package org.wikimedia.flink

import java.lang

import org.apache.flink.api.common.functions.{RichFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation

object StateDefinition {
  val STATE_OP_UUID = "simple-state"

  def openState(runtimeContext: RuntimeContext): ValueState[lang.Long] = {
    val simpleState = new ValueStateDescriptor[lang.Long]("simple_state", createTypeInformation[lang.Long])
    runtimeContext.getState(simpleState)
  }
}
