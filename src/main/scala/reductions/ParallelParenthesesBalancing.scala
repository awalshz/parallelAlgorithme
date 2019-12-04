package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000

    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")

    println("done")

  }

}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def balanceAcc(chars: Array[Char], Acc: Int): Boolean = {
      if (chars.isEmpty) Acc == 0
      else if ((Acc == 0) && chars(0) == ')') false
      else if (chars(0) == ')') balanceAcc(chars.tail, Acc - 1)
      else if (chars(0) == '(') balanceAcc(chars.tail, Acc + 1)
      else balanceAcc(chars.tail, Acc)
    }
    balanceAcc(chars, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      def op(n: (Int, Int), c: Char): (Int, Int) = {
        if (c == '(') (n._1, n._2 + 1)
        else if ((c == ')') && (n._2 == 0)) (n._1 + 1, n._2)
        else if (c == ')') (n._1, n._2 - 1)
        else n
      }
      chars.slice(idx, until).foldLeft((arg1, arg2))(op)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else { val (c1, c2) = parallel(reduce(from, (until + from) / 2), reduce((until + from) / 2, until))
              if (c1._2 <= c2._1) (c1._1 + c2._1 - c1._2, c2._2)
              else (c1._1, c2._2 + c1._2 - c2._1)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
