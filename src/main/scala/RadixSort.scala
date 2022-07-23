import scala.collection.mutable
import scala.collection.mutable.ArraySeq
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Success

object RadixSort extends App {

  def radixSortSeq(a: Array[Int], max_digits: Int): Array[Int] = {

    def key(value: Int, digit: Int): Int = {
      (value % (scala.math.pow(10, digit))).toInt / scala.math.pow(10, digit-1).toInt
    }

    var from = a
    var to = new Array[Int](a.length)

    for (digit <- 1 to max_digits) {
      val count  = new Array[Int](10)
      from.foreach( (e: Int) => count(key(e, digit)) += 1 )

      for (i <- 1 until 10) {
        count(i) += count(i-1)
      }

      for (e <- from.reverseIterator) {
        count(key(e, digit)) -= 1
        to(count(key(e, digit))) = e
      }
      from = to
      to = new Array[Int](a.length)
    }
    from
  }

  class MutArray(a: mutable.ArraySeq[Int]) {
    private val count: mutable.ArraySeq[Int] = a

    def this(n: Int) = {
      this(Array.ofDim[Int](n))
    }

    def increment(index: Int): Unit = this.synchronized { this.count(index) += 1 }
    def decrement(index: Int): Unit = this.synchronized { this.count(index) -= 1 }
    def update(index: Int, elem: Int): Unit = this.synchronized { this.count(index) = elem }
    def getElt(index: Int): Int = this.synchronized { this.count(index) }
    def get(): Vector[Int] = this.synchronized { this.count.toVector }
  }

  def radixSortPar(a: Vector[Int]): Vector[Int] = {
    val output = new MutArray(a.toArray)
    val max = a.max;

    def radixHelper(a_helper: Vector[Int], n: Int, place: Int): Vector[Int] = {
      if max / place <= 0 then a_helper
      else radixHelper(countingSort(output, place), n, place * 10)
    }
    radixHelper(a, a.length, 1)
  }

  def countingSort(a: MutArray, place: Int): Vector[Int] = {
    val count = par_count(a, place)
    val h = count.head

    val sumCount = count.drop(1).scanLeft(h)((s,x) => s + x)
    par_place(a, sumCount.toArray, place)
    a.get()
  }

  def par_place(a: MutArray, count: mutable.ArraySeq[Int], place: Int): Vector[Int] = {
    val NUM_GROUPS = 2048
    val groups = a.get().grouped(NUM_GROUPS).toVector
    val units = groups.map(rng => Future {
      for (i <- Range(rng.length-1, -1, -1)) {
        a.update(count((rng(i) / place) % 10) - 1, rng(i))
        count((rng(i) / place) % 10) -= 1
      }
    })
    val flattened = Future.sequence(units)

    Await.result(flattened, Duration.Inf)
    a.get()
  }

  def par_count(a: MutArray, place: Int): Vector[Int] = {
    val count = new MutArray(10)

    val NUM_GROUPS = 2048
    val groups = a.get().grouped(NUM_GROUPS).toVector
    val units = groups.map(rng => Future {
      for (elem <- rng) { count.increment((elem / place) % 10) }
    })
    val flattened = Future.sequence(units)

    Await.result(flattened, Duration.Inf)
    count.get()
  }

  def timed[A](f: => A): Double = {
    val start = System.nanoTime
    val res = f
    val stop = System.nanoTime
    (stop - start)/1e9
  }

  val arr = Range(1000000, 0, -1)
  println(timed(radixSortSeq(arr.toArray, 7).toVector))
  println(timed(radixSortPar(arr.toVector)))
}
