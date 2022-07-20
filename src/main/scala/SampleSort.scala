import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Success
import scala.util.Random
import scala.util.control.Breaks._

object SampleSort extends App {

  def quickSort(xs: Array[Int]): Array[Int] = {
    if (xs.length <= 1) xs
    else {
      val pivot = xs(xs.length / 2)
      Array.concat(
        quickSort(xs.filter(_ < pivot)),
        xs.filter(_ == pivot),
        quickSort(xs.filter(_ > pivot)))
    }
  }

  def parQuickSort(xs: Array[Int]): Future[Array[Int]] = {
    if (xs.length <= 1) Future { xs }
    else {
      val pivot = xs(xs.length / 2)
      val NUM_GROUPS = 2048
      val n = xs.length

      val groups = xs.grouped(n / NUM_GROUPS).toVector
      val lt = groups.map(rng => Future { quickSort(rng.filter(_ < pivot)) })
      val lt_flat = Future.sequence(lt)

      val groups2 = xs.grouped(n / NUM_GROUPS).toVector
      val eq = groups2.map(rng => Future{ rng.filter(_ == pivot) })
      val eq_flat = Future.sequence(eq)

      val groups3 = xs.grouped(n / NUM_GROUPS).toVector
      val gt = groups3.map(rng => Future{ quickSort(rng.filter(_ > pivot)) })
      val gt_flat = Future.sequence(gt)

      for (
        res <- lt_flat;
        res1 <- eq_flat;
        res2 <- gt_flat
      ) yield (res.flatten ++ res1.flatten ++ res2.flatten).toArray

    }
  }

  // a is the input array, k is the oversampling factor, p is the number of buckets
  // The oversampling ratio k determines the number of data elements the sample should have.
  // If input data is widely distributed (not many duplicate values) then small sampling
  // ratio is sufficient, otherwise (many duplicates / not well distributed data) then larger ratio is better
  def sampleSort(a: Vector[Int], k: Int, p: Int): Vector[Int] = {
    // If the bucket size is too small (< 5), switch to quick-sorting the input array
    if a.length / k < 5 then return quickSort(a.toArray).toVector

    val random = new Random
    val tmp = a
    var sample: Vector[Int] = Vector()

    for (_ <- Range(1, (p - 1)*k+1)) {
      sample = sample :+ tmp(random.nextInt(tmp.length))
    }
    val sorted = quickSort(sample.toArray)

    var splitters = Vector(Int.MinValue) // These are the splitters
    for (i <- Range(k-1, (p - 1)*k+k-1, k)) {
      splitters = splitters :+ sorted(i)
    }
    splitters = splitters :+ Int.MaxValue

    var buckets = Vector.fill(p, 0)(0)
    for (elt <- a) {
      breakable {
        for (i <- Range(1, p+1)) {
          if elt > splitters(i - 1) && elt <= splitters(i) then {
            buckets = buckets.updated(i-1, buckets(i-1) :+ elt)
            break
          }
        }
      }
    }

    val futureBuckets = buckets.map(rng => Future {
      quickSort(rng.toArray)
    })
    val flattened = Future.sequence(futureBuckets)
    val sortedBuckets = Await.result(flattened, Duration.Inf)
    sortedBuckets.flatten
  }

  val vec = Vector(6,2,4,634,34,135,3451,1,23,235,77,3,2,45,12,5,47,4,22,5,62,23,4567,3,23,25,3645646,2345,564562,
                  4,234,53,456,54645,64,4656,234,2454,63425,4,376,7876,86968,22)
  println(sampleSort(vec, 5, 10))
  println(sampleSort(Vector(5,4,3,2,1,45,23,1,423,234,23,23,5,3,6,2,1), 3, 10))
  //println(quickSort(Array(5, 4, 3, 2, 1)).mkString("Array(", ", ", ")"))
}
