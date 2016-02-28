import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import scala.language.postfixOps
import scala.io.StdIn
import scala.util._
import scala.util.control.NonFatal
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global


val working = Future.run() { ct =>
  Future {
    while (ct.nonCancelled) {
      println("working")
    }
    println("done")
  }
}

Future.delay(5 seconds) onSuccess {
  case _ => working.unsubscribe()
}