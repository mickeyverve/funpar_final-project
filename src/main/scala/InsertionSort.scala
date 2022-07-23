import scala.collection.mutable
import scala.collection.mutable.ArraySeq
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Success

object InsertionSort extends App {

  def insertionSortSeq(array: Vector[Int]): Vector[Int] = {
    var key = 0
    var j = 0
    var sorted = array

    for (i <- Range(1, array.length)) {
      key = array(i)
      j = i - 1

      var tmp = 0
      while (j >= 0 && key < sorted(j)) {
        tmp = sorted(j + 1)
        sorted = sorted.updated(j + 1, sorted(j))
        sorted = sorted.updated(j, tmp)
        j = j - 1
      }

      sorted = sorted.updated(j + 1, key)
    }
    sorted
  }

  def insertionSortPar(array: Vector[Int]): Vector[Int] = {
    var sorted = array
    var key = 0
    var j = 0

    if array.length <= 5 then {
      for (i <- Range(1, array.length)) {
        key = array(i)
        j = i - 1
        var tmp = 0
        while (j >= 0 && key < sorted(j)) {
          tmp = sorted(j + 1)
          sorted = sorted.updated(j + 1, sorted(j))
          sorted = sorted.updated(j, tmp)
          j = j - 1
        }
      }
    }
    else {
      val m = array.length / 2
      val futures = Vector(Future{ insertionSortPar(array.dropRight(m)) }, Future{ insertionSortPar(array.drop(m+1)) })
      val flattened = Future.sequence(futures)
      val result = Await.result(flattened, Duration.Inf)
      merge(result(0), result(1))
    }

    def merge(left: Vector[Int], right: Vector[Int]): Vector[Int] = {
      var mutLeft = left
      var mutRight = right
      var merged = left ++ right
      var m = 0
      if mutLeft.length <= 5 then {
        if mutLeft(mutLeft.length-1) > mutRight(0) then {
          for (i <- Range(0, right.length)) {
            key = mutRight(i)
            m = i - 1
            while (m >= 0 && mutRight(m) > key) {
              mutRight = mutRight.updated(m + 1, mutRight(m))
              m = m - 1
            }
            if mutLeft(mutLeft.length - 1) > key then {
              mutRight = mutRight.updated(0, mutLeft(mutLeft.length - 1))
              m = mutLeft.length - 2
              while (m >= 0 && mutLeft(m) > key) {
                mutLeft = mutLeft.updated(m + 1, mutLeft(m))
                m = m - 1
              }
            }
            merged = merged.updated(m + 1, key)
          }
        }
      }
      else {
        val leftM = mutLeft.length / 2
        val rightM = right.length / 2
        val futures = Vector(Future{ merge(mutLeft.dropRight(leftM), mutRight.dropRight(rightM)) },
                            Future{ merge(mutLeft.drop(leftM + 1), mutRight.drop(rightM + 1)) })
        val flattened = Future.sequence(futures)
        val result = Await.result(flattened, Duration.Inf)
        merge(result(0), result(1))
      }
      merged
    }
    sorted.reverse
  }

  def timed[A](f: => A): (A, Double) = {
    val start = System.nanoTime
    val res = f
    val stop = System.nanoTime
    (res, (stop - start)/1e9)
  }

  val arr = Range(10,0,-1).toVector
  println(timed(insertionSortSeq(arr)))
  println(timed(insertionSortPar(arr)))

}
