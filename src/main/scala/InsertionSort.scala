object InsertionSort extends App{
  def timed[A](f: => A): (A, Double) = {
    val start = System.nanoTime
    val res = f
    val stop = System.nanoTime
    (res, (stop - start)/1e9)
  }
  def insertionSort(xs: List[Int]): List[Int] =
    if (xs.isEmpty) Nil
    else insert(xs.head, insertionSort(xs.tail))

  def insert(x: Int, xs: List[Int]): List[Int] =
    if (xs.isEmpty || x <= xs.head) x :: xs
    else xs.head :: insert(x, xs.tail)

  val r = scala.util.Random
  val randomArray = (for (i <- 1 to 30000) yield r.nextInt(10000)).toList
  println(timed(insertionSort(randomArray)))
}