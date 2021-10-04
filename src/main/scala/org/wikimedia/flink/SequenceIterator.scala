package org.wikimedia.flink

import java.{lang, util}

import org.apache.flink.api.java.tuple.Tuple2

class SequenceIterator(size: Int, reversed: Boolean = false, seed: Long = 123) extends util.Iterator[Tuple2[String, lang.Long]] with Serializable {
  var cur: Int = 0
  lazy val rand = new util.Random(seed)

  override def hasNext: Boolean = cur < size

  override def next(): Tuple2[String, lang.Long] = {
    if (cur >= size) {
      throw new NoSuchElementException()
    }
    cur += 1
    val key = if (reversed) {size - cur + 1} else {cur}
    new Tuple2("K" + key, rand.nextLong())
  }
}
