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
  
  // a is the input array, k is the oversampling factor, p is the number of buckets
  // The oversampling ratio k determines the number of data elements the sample should have.
  // If input data is widely distributed (not many duplicate values) then small sampling
  // ratio is sufficient, otherwise (many duplicates / not well distributed data) then larger ratio is better
  def sampleSortPar(a: Vector[Int], k: Int, p: Int): Vector[Int] = {
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

  def sampleSortSeq(a: Vector[Int], k: Int, p: Int): Vector[Int] = {

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

    val sortedBuckets = buckets.map(rng => quickSort(rng.toArray))
    sortedBuckets.flatten
  }

  def timed[A](f: => A): Double = {
    val start = System.nanoTime
    val res = f
    val stop = System.nanoTime
    (stop - start)/1e9
  }

  val r = scala.util.Random
  val randomArray = (for (i <- 1 to 10000000) yield r.nextInt(10000)).toArray

  println(timed(sampleSortSeq(randomArray.toVector, 5, 10)))
  println(timed(sampleSortPar(randomArray.toVector, 5, 10)))
}
