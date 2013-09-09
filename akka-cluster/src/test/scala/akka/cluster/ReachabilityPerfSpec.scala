/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.cluster

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import akka.actor.Address

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ReachabilityPerfSpec extends WordSpec with ShouldMatchers {

  val nodesSize = sys.props.get("akka.cluster.ReachabilityPerfSpec.clockSize").getOrElse("1000").toInt
  val iterations = sys.props.get("akka.cluster.ReachabilityPerfSpec.iterations").getOrElse("10000").toInt

  val address = Address("akka.tcp", "sys", "a", 2552)
  val node = Address("akka.tcp", "sys", "a", 2552)

  def createReachabilityOfSize(base: Reachability, size: Int): Reachability =
    (base /: (1 to size)) {
      case (r, i) ⇒
        val observer = UniqueAddress(address.copy(host = Some("node-" + i)), i)
        val j = if (i == size) 1 else i + 1
        val subject = UniqueAddress(address.copy(host = Some("node-" + j)), j)
        r.unreachable(observer, subject).reachable(observer, subject)
    }

  def addUnreachable(base: Reachability, count: Int): Reachability = {
    val observers = base.allObservers.take(count)
    val subjects = Stream.continually(base.allObservers).flatten.iterator
    (base /: observers) {
      case (r, o) ⇒
        (r /: (1 to 5)) { case (r, _) ⇒ r.unreachable(o, subjects.next()) }
    }
  }

  def checkThunkFor(r1: Reachability, r2: Reachability, thunk: (Reachability, Reachability) ⇒ Unit, times: Int): Unit = {
    for (i ← 1 to times) {
      thunk(r1, r2)
    }
  }

  val r1 = createReachabilityOfSize(Reachability.empty, nodesSize)
  val r2 = createReachabilityOfSize(r1, nodesSize)
  val r3 = addUnreachable(r1, nodesSize / 2)
  val allowed = r1.allObservers

  def merge(expectedRecords: Int)(r1: Reachability, r2: Reachability): Unit = {
    r1.merge(allowed, r2).records.size should be(expectedRecords)
  }

  def !==(vc1: VectorClock, vc2: VectorClock): Unit = {
    vc1 == vc2 should be(false)
  }

  s"Reachability merge of size $nodesSize" must {

    s"do a warm up run, $iterations times" in {
      checkThunkFor(r1, r2, merge(0), iterations)
    }

    s"merge with same versions, $iterations times" in {
      checkThunkFor(r1, r1, merge(0), iterations)
    }

    s"merge with all older versions, $iterations times" in {
      checkThunkFor(r2, r1, merge(0), iterations)
    }

    s"merge with all newer versions, $iterations times" in {
      checkThunkFor(r1, r2, merge(0), iterations)
    }

    s"merge with half nodes unreachable, $iterations times" in {
      checkThunkFor(r1, r3, merge(5 * nodesSize / 2), iterations)
    }

    s"merge with half nodes unreachable opposite $iterations times" in {
      checkThunkFor(r3, r1, merge(5 * nodesSize / 2), iterations)
    }
  }
}
