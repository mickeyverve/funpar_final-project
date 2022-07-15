import scala.collection.mutable
import scala.collection.mutable.ArraySeq
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Success

object RadixSort extends App {

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

  def radixSort(a: Vector[Int]): Vector[Int] = {
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

  println(radixSort(Vector(45,23,12,11,64,31,1234, 231233,4234,23, 12, 3123, 131)))
}
