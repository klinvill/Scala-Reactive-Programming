package nodescala

import nodescala.NodeScala._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.collection._
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

import scala.async.Async._
import scala.util.Try

import ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be completed") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }
  test("A Future should never be completed") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("All futures return") {
    val list: List[Future[Int]] = List(
      Future.always(1),
      Future.always(2),
      Future.delay(1 second) continueWith { _ => 3 }
    )
    val all: Future[List[Int]] = Future.all(list)
    assert(Await.result(all, 1.1 seconds).sum == 6)
  }

  test("Any future finishes first") {
    val list = List(Future.always(1), Future.always(2), Future.never[Int])
    assert(list.map(f => f.value).contains(Future.any(list).value))
  }

  test("any method works HAPPY") {
    val l = List(Future { Thread.sleep(200); 200 }, Future { Thread.sleep(150); 150 }, Future { Thread.sleep(199); 199 })
    val r = Await.result(Future.any(l), 300 millis)
    assert(r == 150)
  }

  test("Lets delay that future - 1 sec") {
    val x = Future.delay(4 seconds)
    assert(x.isCompleted === false)
  }

  test("Lets delay that future again - 3.9 sec") {
    val x = Future.delay(4 seconds)
    blocking(Thread.sleep(3900L))
    assert(x.isCompleted === false)
  }

  test("Lets delay that future AGAIN - 5 sec") {
    val x = Future.delay(4 seconds)
    blocking(Thread.sleep(5000L))
    assert(x.isCompleted === true)
  }

  test("run") {
    val working = Future.run() { ct =>
      async {
        while (ct.nonCancelled) {
          println("working")
          blocking(Thread.sleep(500L))
        }
        println("done")
      }
    }

    Future.delay(5 seconds) onSuccess {
      case _ => working.unsubscribe()
    }
    blocking(Thread.sleep(6000L))
  }


  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }
  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

}




