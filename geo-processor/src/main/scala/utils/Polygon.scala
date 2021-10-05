package utils

import java.awt

class Polygon(private val polygonPoints: Array[Array[Double]]) {
  def containsPointRayCastingImpl1(point: Array[Double]): Boolean = {
    val xs: Array[Double] = for (line <- polygonPoints) yield line(0)
    val ys: Array[Double] = for (line <- polygonPoints) yield line(1)

    val max_x = xs.max
    val min_x = xs.min
    val max_y = ys.max
    val min_y = ys.min

    if (point(0) > max_x || point(0) < min_x || point(1) > max_y || point(1) < min_y)
      return false

    if (polygonPoints.exists((p) => {p(0) == point(0) && p(1) == point(1)}))
      return true

    val epsilon: Double = 1
    val ray = Array(point, Array(max_x + epsilon, max_y + epsilon))
    val polyLines = polygonPoints.zip(polygonPoints.tail)
    val intersections: Array[Int] = for (polyLine <- polyLines) yield intersects(polyLine, ray)
    val numberOfIntersections = intersections.sum

    if (numberOfIntersections % 2 == 1)
      true
    else
      false
  }

  private def intersects(line1: (Array[Double], Array[Double]), line2: Array[Array[Double]]): Int = {
    val p1: Array[Double] = line1._1
    val q1: Array[Double] = line1._2

    val p2: Array[Double] = line2(0)
    val q2: Array[Double] = line2(1)

    val o1: Int = orientation(p1, q1, p2)
    val o2: Int = orientation(p1, q1, q2)
    val o3: Int = orientation(p2, q2, p1)
    val o4: Int = orientation(p2, q2, q1)

    if (o1 != o2 && o3 != o4) return 1
    if (o1 == 0 && onSegment(p1, q1, p2)) return 1
    if (o2 == 0 && onSegment(p1, q1, q2)) return 1
    if (o3 == 0 && onSegment(p2, q2, p1)) return 1
    if (o4 == 0 && onSegment(p2, q2, q1)) return 1

    0
  }

  private def orientation(p: Array[Double], q: Array[Double], r: Array[Double]): Int = {
    val tmp = ((q(1) - p(1)) * (r(0) - q(0))) - ((q(0) - p(0)) * (r(1) - q(1)))
    if (tmp == 0) 0
    else if (tmp > 0) 1
    else -1
  }

  private def onSegment(p: Array[Double], q: Array[Double], r: Array[Double]): Boolean = {
    if (
      r(0) <= p(0).max(q(0))
        && r(0) >= p(0).min(q(0))
        && r(1) <= p(1).max(q(1))
        && r(1) >= p(1).min(q(1))
    ) return true

    false
  }

  def containsPointRayCastingImpl2(point: Array[Double]): Boolean = {
    var crossings = 0

    for (i <- 0 to (polygonPoints.length - 2)) {
      val j = i + 1

      val cond1: Boolean = (polygonPoints(i)(1) <= point(1)) && (point(1) < polygonPoints(j)(1))
      val cond2: Boolean = (polygonPoints(j)(1) <= point(1)) && (point(1) < polygonPoints(i)(1))

      if (cond1 || cond2)
        if (point(0) < (polygonPoints(j)(0) - polygonPoints(i)(0)) * (point(1) - polygonPoints(i)(1))
          / (polygonPoints(j)(1) - polygonPoints(i)(1)) + polygonPoints(i)(0)) {
          crossings += 1
        }
    }

    if (crossings % 2 == 1) true
    else false
  }

  def containsPointSumOfAngles(point: Array[Double]): Boolean = {
    var angle = 0.0
    val p1 = Array(0.0, 0.0)
    val p2 = Array(0.0, 0.0)
    var i = 0
    val n = polygonPoints.length

    while (i < n) {
      p1(0) = polygonPoints(i)(0) - point(0)
      p1(1) = polygonPoints(i)(1) - point(1)
      p2(0) = polygonPoints((i + 1) % n)(0) - point(0)
      p2(1) = polygonPoints((i + 1) % n)(1) - point(1)
      angle += angle2D(p1(0), p1(1), p2(0), p2(1))

      i += 1
    }

    if (scala.math.abs(angle) < scala.math.Pi)
      false
    else
      true
  }

  private def angle2D(x1: Double, y1: Double, x2: Double, y2: Double): Double = {
    val theta1 = scala.math.atan2(y1, x1)
    val theta2 = scala.math.atan2(y2, x2)
    var dtheta = theta2 - theta1
    while (dtheta > scala.math.Pi)
      dtheta -= 2 * scala.math.Pi
    while (dtheta < -scala.math.Pi)
      dtheta += 2 * scala.math.Pi

    dtheta
  }

  def containsPointAwt(point: Array[Double]): Boolean = {
    val p: awt.Polygon = new awt.Polygon()
    for (points <- polygonPoints) p.addPoint((points(0) * 100).round.toInt, (points(1) * 100).round.toInt)

    p.contains((point(0) * 100).toInt, (point(1) * 100).toInt)
  }
}
