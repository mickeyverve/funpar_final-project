import scala.annotation.tailrec
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Success

object SelectionSort extends App{

  def timed[A](f: => A): (A, Double) = {
    val start = System.nanoTime
    val res = f
    val stop = System.nanoTime
    (res, (stop - start)/1e9)
  }

  def selectionSort(list:List[Int]): Future[List[Int]] = {
    @tailrec
    def selectSortHelper(list:List[Int], accumList:List[Int] = List[Int]()): Future[List[Int]] = {

      list match {
        case Nil => Future { accumList }
        case _ =>
          val min  = Await.result(par_min(list), Duration.Inf)
          val requiredList = Await.result(par_filter(list, min), Duration.Inf)
          selectSortHelper(requiredList, accumList ::: List.fill(list.length - requiredList.length)(min))
      }
    }
    selectSortHelper(list)
  }

  def par_min(list: List[Int]): Future[Int] = {
    val NUM_GROUPS = 2048
    val groups = list.grouped(NUM_GROUPS).toVector
    val grouped = groups.map(rng => Future { find_min(rng, Int.MaxValue) })
    val flat = Future.sequence(grouped)

    for (
      res <- flat
    ) yield find_min(res.toList, Int.MaxValue)
  }
  def find_min(list: List[Int], min: Int): Int = list match {
    case Nil => min
    case h::t => if min > h then find_min(t, h) else find_min(t, min)
  }

  def par_filter(list: List[Int], min: Int): Future[List[Int]] = {
    val NUM_GROUPS = 2048
    val groups = list.grouped(NUM_GROUPS).toVector
    val grouped = groups.map(rng => Future { rng.filter(_ != min) })
    val flat = Future.sequence(grouped)

    for (
      res <- flat
    ) yield res.flatten.toList
  }

  val r = scala.util.Random
  val randomArray = (for (i <- 1 to 300000) yield r.nextInt(10000)).toList
  println(timed(selectionSort(randomArray)))


  //println(timed(Await.result(par_min(randomArray), Duration.Inf)))
  //println(timed(randomArray.filter(_ != 1)))
}
