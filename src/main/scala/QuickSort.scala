import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Success

object QuickSort extends App {

  def quickSortSeq(xs: Array[Int]): Array[Int] = {
    if (xs.length <= 1) xs
    else {
      val pivot = xs(xs.length / 2)
      Array.concat(
        quickSortSeq(xs.filter(_ < pivot)),
        xs.filter(_ == pivot),
        quickSortSeq(xs.filter(_ > pivot)))
    }
  }

  def quickSortPar(xs: Array[Int]): Future[Array[Int]] = {
    if (xs.length <= 1) Future { xs }
    else {
      val pivot = xs(xs.length / 2)
      val NUM_GROUPS = 2048
      val n = xs.length

      val groups = xs.grouped(n / NUM_GROUPS).toVector
      val lt = groups.map(rng => Future { quickSortSeq(rng.filter(_ < pivot)) })
      val lt_flat = Future.sequence(lt)

      val groups2 = xs.grouped(n / NUM_GROUPS).toVector
      val eq = groups2.map(rng => Future{ rng.filter(_ == pivot) })
      val eq_flat = Future.sequence(eq)

      val groups3 = xs.grouped(n / NUM_GROUPS).toVector
      val gt = groups3.map(rng => Future{ quickSortSeq(rng.filter(_ > pivot)) })
      val gt_flat = Future.sequence(gt)

      for (
        res <- lt_flat;
        res1 <- eq_flat;
        res2 <- gt_flat
      ) yield (res.flatten ++ res1.flatten ++ res2.flatten).toArray

    }
  }

  def timed[A](f: => A): Double = {
    val start = System.nanoTime
    val res = f
    val stop = System.nanoTime
    (stop - start)/1e9
  }

  val r = scala.util.Random
  val randomArray = (for (i <- 1 to 10000000) yield r.nextInt(10000)).toArray

  println(timed(quickSortSeq(randomArray).mkString(", ")))
  println(timed(Await.result(quickSortPar(randomArray), Duration.Inf).mkString(", ")))


}
