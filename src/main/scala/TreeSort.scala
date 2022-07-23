import scala.collection.mutable
import scala.collection.mutable.ArraySeq
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Success

abstract class Node
case class LeafNode(data: Int) extends Node;
case class FullNode(data: Int, left: Node, right: Node) extends Node
case class LeftNode(data: Int, left: Node) extends Node
case class RightNode(data: Int, right: Node) extends Node

object TreeSort extends App {

  // Sequential Version of TreeSort
  def treeSortSeq(arr: Vector[Int]): Vector[Int] = {

    // Creates a binary search tree from list of strings
    def construct(arr: Vector[Int]): Node = {
      def insert(tree: Node, value: Int): Node = {
        tree match {
          case null => LeafNode(value)
          case LeafNode(data) => if (value > data) {
            LeftNode(data, LeafNode(value))
          } else {
            RightNode(data, LeafNode(value))
          }
          case LeftNode(data, left) => if (value > data) {
            LeftNode(value, LeftNode(data, left))
          } else {
            FullNode(data, left, LeafNode(value))
          }
          case RightNode(data, right) => if (value > data) {
            FullNode(data, LeafNode(value), right)
          } else {
            RightNode(value, RightNode(data, right))
          }
          case FullNode(data, left, right) => if (value > data) {
            FullNode(data, insert(left, value), right)
          } else {
            FullNode(data, left, insert(right, value))
          }
        }
      }

      var tree: Node = null;

      for (item <- arr) {
        tree = insert(tree, item)
      }

      tree
    }

    def sortedRender(A: Node): Vector[Int] = {
      A match {
        case null => {
          Vector()
        }
        case LeafNode(data) => {
          Vector(data)
        }
        case FullNode(data, left, right) => {
          sortedRender(left) ++
            Vector(data) ++
            sortedRender(right)
        }
        case RightNode(data, right) => {
            Vector(data) ++
            sortedRender(right)
        }
        case LeftNode(data, left) => {
          sortedRender(left) ++
            Vector(data)
        }
      }
    }

    sortedRender(construct(arr))
  }


  // Parallel Version of TreeSort
  def treeSortPar(arr: Vector[Int]): Vector[Int] = {

    // Creates a binary search tree from list of strings
    def construct(arr: Vector[Int]): Node = {
      def insert(tree: Node, value: Int): Node = {
        tree match {
          case null => LeafNode(value)
          case LeafNode(data) => if (value > data) {
            LeftNode(data, LeafNode(value))
          } else {
            RightNode(data, LeafNode(value))
          }
          case LeftNode(data, left) => if (value > data) {
            LeftNode(value, LeftNode(data, left))
          } else {
            FullNode(data, left, LeafNode(value))
          }
          case RightNode(data, right) => if (value > data) {
            FullNode(data, LeafNode(value), right)
          } else {
            RightNode(value, RightNode(data, right))
          }
          case FullNode(data, left, right) => if (value > data) {
            FullNode(data, insert(left, value), right)
          } else {
            FullNode(data, left, insert(right, value))
          }
        }
      }

      var tree: Node = null;

      for (item <- arr) {
        tree = insert(tree, item)
      }

      tree
    }

    def sortedRender(A: Node): Vector[Int] = {
      A match {
        case null => {
          Vector()
        }
        case LeafNode(data) => {
          Vector(data)
        }
        case FullNode(data, left, right) => {
          val futures = Vector(Future{ sortedRender(left) }, Future{ sortedRender(right) })
          val flattened = Future.sequence(futures)
          val result = Await.result(flattened, Duration.Inf)
          (result(0) :+ data) ++ result(1)
        }
        case RightNode(data, right) => {
          Vector(data) ++
            sortedRender(right)
        }
        case LeftNode(data, left) => {
          sortedRender(left) ++
            Vector(data)
        }
      }
    }

    sortedRender(construct(arr))
  }

  def timed[A](f: => A): (A, Double) = {
    val start = System.nanoTime
    val res = f
    val stop = System.nanoTime
    (res, (stop - start)/1e9)
  }

  val arr = Range(1001, 0, -1)

  println(timed(treeSortSeq(arr.toVector)))

  val arr1 = Range(1001, 0, -1)

  println(timed(treeSortPar(arr1.toVector)))
}