import scala.collection.mutable
import scala.collection.mutable.ArraySeq
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Success

object RadixSort extends App {

  class Counter(n: Int) {
    private val count: mutable.ArraySeq[Int] = Array.ofDim[Int](n)

    def increment(index: Int): Unit = this.synchronized { this.count(index) += 1 }
    def getElt(index: Int): Int = this.synchronized { this.count(index) }
    def get(): Vector[Int] = this.synchronized { this.count.toVector }
  }

  def radixSort(array: Vector[Int]): Vector[Int] = {
    val max = array.max.toString.length;

    def radixHelper(array: Vector[Int], n: Int, place: Int): Vector[Int] = {
      if max / place > 0 then array
      else radixHelper(countingSort(array, n, place), n, place * 10)
    }
    radixHelper(array, array.length, 1)
  }

  def countingSort(array: Vector[Int], n: Int, place: Int): Vector[Int] = {

    val count = par_count(array, n, place)
    val output = Vector.range(0, n)

    println(count.scanLeft(0)((s,x) => s + x))
    count

  }

  def par_place(count: Counter, output: Counter, n: Int, place: Int): Vector[Int] = {
    val NUM_GROUPS = 2048
    val groups = count.get().grouped(NUM_GROUPS).toVector
    val units = groups.map(rng => Future {
      for (elem <- rng) {
        output.get()[count.getElt((elem / place) % 10) - 1] = elem
        count.
      }
    })
    val flattened = Future.sequence(units)

    Await.result(flattened, Duration.Inf)
    /* */
    count.get()
  }

  def par_count(array: Vector[Int], n: Int, place: Int): Vector[Int] = {
    val count = new Counter(n)

    val NUM_GROUPS = 2048
    val groups = array.grouped(NUM_GROUPS).toVector
    val units = groups.map(rng => Future {
      for (elem <- rng) { count.increment((elem / place) % 10) }
    })
    val flattened = Future.sequence(units)

    Await.result(flattened, Duration.Inf)
    /* */
    count.get()
  }

  println(countingSort(Vector(45,23,12,11,64,31), 6, 1))

  //https://www.javatpoint.com/radix-sort
}
