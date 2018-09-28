/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.mllib.clustering.dbscan

import scala.annotation.tailrec
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Seq
import scala.math._

/**
  * Helper methods for calling the partitioner
  */
object EvenSplitPartitioner {

  def partition(
                 toSplit: Set[(DBSCANRectangle, Int)],
                 maxPointsPerPartition: Long,
                 minimumRectangleSize: Double): List[(DBSCANRectangle, Int)] = {
    new EvenSplitPartitioner(maxPointsPerPartition, minimumRectangleSize)
      .findPartitions(toSplit)
  }

}

class EvenSplitPartitioner(
                            maxPointsPerPartition: Long,
                            minimumRectangleSize: Double) extends Logging {

  type RectangleWithCount = (DBSCANRectangle, Int)

  def findPartitions(toSplit: Set[RectangleWithCount]): List[RectangleWithCount] = {

    val boundingRectangle = findBoundingRectangle(toSplit)

    def pointsIn = pointsInRectangle(toSplit, _: DBSCANRectangle)

    val toPartition = List((boundingRectangle, pointsIn(boundingRectangle)))
    val partitioned = List[RectangleWithCount]()

    logTrace("About to start partitioning")
    val partitions = partition(toPartition, partitioned, pointsIn)
    logTrace("Done")

    // remove empty partitions
    partitions.filter({ case (partition, count) => count > 0 })
  }

  @tailrec
  private def partition(
                         remaining: List[RectangleWithCount],
                         partitioned: List[RectangleWithCount],
                         pointsIn: (DBSCANRectangle) => Int): List[RectangleWithCount] = {

    remaining match {
      case (rectangle, count) :: rest =>
        if (count > maxPointsPerPartition) {

          if (canBeSplit(rectangle)) {
            logTrace(s"About to split: $rectangle")

            def cost = (r: DBSCANRectangle) => ((pointsIn(rectangle) / 2) - pointsIn(r)).abs

            val (split1, split2) = split(rectangle, cost)
            logTrace(s"Found split: $split1, $split2")
            val s1 = (split1, pointsIn(split1))
            val s2 = (split2, pointsIn(split2))
            partition(s1 :: s2 :: rest, partitioned, pointsIn)

          } else {
            logWarning(s"Can't split: ($rectangle -> $count) (maxSize: $maxPointsPerPartition)")
            partition(rest, (rectangle, count) :: partitioned, pointsIn)
          }

        } else {
          partition(rest, (rectangle, count) :: partitioned, pointsIn)
        }

      case Nil => partitioned

    }

  }

  def split(
             rectangle: DBSCANRectangle,
             cost: (DBSCANRectangle) => Int): (DBSCANRectangle, DBSCANRectangle) = {

    val smallestSplit =
      findPossibleSplits(rectangle)
        .reduceLeft {
          (smallest, current) =>

            if (cost(current) < cost(smallest)) {
              current
            } else {
              smallest
            }

        }

    (smallestSplit, (complement(smallestSplit, rectangle)))

  }

  /**
    * Returns the box that covers the space inside boundary that is not covered by box
    */
  private def complement(box: DBSCANRectangle, boundary: DBSCANRectangle): DBSCANRectangle =
  //    if (box.x == boundary.x && box.y == boundary.y) {
  //      if (boundary.x2 >= box.x2 && boundary.y2 >= box.y2) {
  //        if (box.y2 == boundary.y2) {
  //          DBSCANRectangle(box.x2, box.y, boundary.x2, boundary.y2)
  //        } else if (box.x2 == boundary.x2) {
  //          DBSCANRectangle(box.x, box.y2, boundary.x2, boundary.y2)
  //        } else {
  //          throw new IllegalArgumentException("rectangle is not a proper sub-rectangle")
  //        }
  //      } else {
  //        throw new IllegalArgumentException("rectangle is smaller than boundary")
  //      }
  //    } else {
  //      throw new IllegalArgumentException("unequal rectangle")
  //    }
    if (box.x == boundary.x && box.y == boundary.y) {
      var new_vector_1 = ArrayBuffer[Double]()
      var new_vector_2 = ArrayBuffer[Double]()
      for (a <- 0 to box.x.size) {
        new_vector_1 += max(box.x(a), boundary.x(a))
        new_vector_2 += max(box.y(a), boundary.y(a))
      }
      DBSCANRectangle(Vectors.dense(new_vector_1.toArray), Vectors.dense(new_vector_2.toArray))
    } else {
      throw new IllegalArgumentException("unequal rectangle")
    }


  /**
    * Returns all the possible ways in which the given box can be split
    */
  private def findPossibleSplits(box: DBSCANRectangle): Set[DBSCANRectangle] = {

    //    val xSplits = (box.x + minimumRectangleSize) until box.x2 by minimumRectangleSize
    //
    //    val ySplits = (box.y + minimumRectangleSize) until box.y2 by minimumRectangleSize
    //
    //    val splits =
    //      xSplits.map(x => DBSCANRectangle(box.x, box.y, x, box.y2)) ++
    //        ySplits.map(y => DBSCANRectangle(box.x, box.y, box.x2, y))
    //
    //    logTrace(s"Possible splits: $splits")
    //
    //    splits.toSet
    import scala.collection.mutable.Map
    var split = new ArrayBuffer[DBSCANRectangle]()
    var maps: Map[Int, Array[Double]] = Map()
    for (a <- 0 to box.x.size) {
      var tmp_list = ((box.x(a) + minimumRectangleSize) until box.y(a) by minimumRectangleSize).toArray
      maps = maps ++ Map(a -> tmp_list)
    }
    for (a <- maps.keys) {
      for (b <- maps.get(a)) {
        var new_list = box.x.toArray.toBuffer
        //        var new_buffer = ArrayBuffer(new_list)
        for (c <- b) {
          new_list(a) = c
          split = split ++ ArrayBuffer(DBSCANRectangle(box.x, Vectors.dense(new_list.toArray)))
        }

      }
    }
    split.toSet
  }

  /**
    * Returns true if the given rectangle can be split into at least two rectangles of minimum size
    */

  import scala.util.control._

  private def canBeSplit(box: DBSCANRectangle): Boolean = {
    //    (box.x2 - box.x > minimumRectangleSize * 2 ||
    //      box.y2 - box.y > minimumRectangleSize * 2)
    val loop = new Breaks

    var mark = !false

    loop.breakable {
      for (a <- 0 to box.x.size) {
        if (box.x(a) - box.y(a) > minimumRectangleSize * 2) {
          mark = true
          loop.break
        } else {
          mark = false
        }
      }
    }
    mark
  }

  def pointsInRectangle(space: Set[RectangleWithCount], rectangle: DBSCANRectangle): Int = {
    space.view
      .filter({ case (current, _) => rectangle.contains(current) })
      .foldLeft(0) {
        case (total, (_, count)) => total + count
      }
  }

  def findBoundingRectangle(rectanglesWithCount: Set[RectangleWithCount]): DBSCANRectangle = {
    val a = rectanglesWithCount.toList(0)._1.x.size
    //    val bounding = new Array[Double](a)
    val minPoint = ArrayBuffer.fill(a)(Double.MaxValue)
    val maxPoint = ArrayBuffer.fill(a)(Double.MinValue)
    for (item <- rectanglesWithCount) {
      for (b <- 0 to a) {
        minPoint(b) = max(min(rectanglesWithCount.toList(0)._1.x.toArray(b), rectanglesWithCount.toList(0)._1.y.toArray(b)), minPoint(b))
        maxPoint(b) = min(max(rectanglesWithCount.toList(0)._1.x.toArray(b), rectanglesWithCount.toList(0)._1.y.toArray(b)), maxPoint(b))
      }
    }
    DBSCANRectangle(Vectors.dense(minPoint.toArray), Vectors.dense(maxPoint.toArray))

    //    val mins = fold(ds, minPoint, x => Math.min(x._1, x._2))
    //    val maxs = fold(ds, maxPoint, x => Math.max(x._1, x._2))
    //
    //    mins.coordinates.zip(maxs.coordinates).map(x => new BoundsInOneDimension(x._1, x._2, true)).toList
    //
    //    val invertedRectangle =
    //      DBSCANRectangle(Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue)
    //
    //    rectanglesWithCount.foldLeft(invertedRectangle) {
    //      case (bounding, (c, _)) =>
    //        DBSCANRectangle(
    //          bounding.x.min(c.x), bounding.y.min(c.y),
    //          bounding.x2.max(c.x2), bounding.y2.max(c.y2))
  }


}
