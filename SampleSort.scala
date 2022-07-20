import scala.reflect.ClassTag
import ca.vgorcinschi.ArrayOps

import Ordered._

//noinspection SpellCheckingInspection
class SampleSort[T: ClassTag : Ordering](val sampleSize: Int = 30) extends QuickSort[T] {

  import SearchTree._

  override def sort(a: Array[T]): Array[T] = {
    require(a != null, "Passed-in array should not be null")
    sortHelper(a)
  }

  private def sortHelper(a: Array[T]): Array[T] = {
    //if the array is shorter then the sampling - sort it with Quicksort
    if (a.length <= sampleSize) return super.sort(a)

    /*
      just the indices for the sample array.
      also required later for figuring out the nonPartitionedRemainder of the array
     */
    val sampleArrayIndices: Array[Int] = a.subArrayOfSize(sampleSize)
    val sampleArray: Array[T] = sampleArrayIndices.map(a(_))

    val sortedSampleArray: Array[T] = sort(sampleArray, 0, sampleArray.length - 1)
    val searchTree: SearchTree = buildTree(sortedSampleArray, sampleSize / 2)
    val nonPartitionedRemainder = a.slice(0, sampleArrayIndices.head) ++ a.slice(sampleArrayIndices.last + 1, a.length)
    val finalTree = (searchTree /: nonPartitionedRemainder) (_ nest _)
    finalTree.arrays() flatMap sort
  }

  private class SearchTree(lt: Array[T], median: Array[T], gt: Array[T]) {
    //hear median is guaranteed to be non null and non empty based off the partitioning in sortHelper
    private val pivot: T = median.head

    def nest(value: T): SearchTree = {
      if (value < pivot) SearchTree(lt :+ value, median, gt)
      if (value > pivot) SearchTree(lt, median, gt :+ value)
      else SearchTree(lt, median :+ value, gt)
    }

    def arrays(): Array[Array[T]] = Array(lt, median, gt)
  }

  private object SearchTree {
    def buildTree(sample: Array[T], pivot: Int): SearchTree = {
      //do not look beyond pivot since sample is guaranteed to be partitioned
      val lt = sample.takeWhile(_ < sample(pivot))
      //only look from pivot and up
      val medianAndGt: (Array[T], Array[T]) = sample.slice(lt.length, sample.length) partition (_ == sample(pivot))
      SearchTree(lt, medianAndGt._1, medianAndGt._2)
    }

    def apply(lt: Array[T], median: Array[T], gt: Array[T]): SearchTree = new SearchTree(lt, median, gt)
  }

}