package lame.index

import intervaltree._
import akka.util.ByteString
import lame.ByteReader
import spire.std.long._

case class Index(
    tree: Tree[IntervalTree.IntervalTreeElement[Long, Index.Block]],
    length: Long
) {
  def query(queryIdx: Long): Option[Long] = {
    IntervalTree
      .lookup1(queryIdx, queryIdx + 1, tree)
      .headOption
      .map(_.virtualFilePointer)
  }
}

object Index {
  private[lame] case class Block(from: Long, to: Long, virtualFilePointer: Long)
      extends LongInterval

  def apply(data: ByteString): Index = {
    val byteReader = new ByteReader(data)
    val iterator = Iterator
      .continually {
        val idx1 = byteReader.readLongLE()
        val idx2 = byteReader.readLongLE()
        val vfp = byteReader.readLongLE()
        val block = Block(idx1,idx2+1, vfp)
        block
      }
      .take(data.size / 24)

    val length = {
      val reader = new ByteReader(data)
      reader.skip(((data.size/24) - 1) * 24 + 8)
      reader.readLongLE() + 1
    }
      
    Index(IntervalTree.makeTree[Long, Block](iterator.toList), length)
  }
}
