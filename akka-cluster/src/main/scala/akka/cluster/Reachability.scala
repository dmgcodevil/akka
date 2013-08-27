/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.cluster

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.breakOut
import akka.actor.Address

/**
 * INTERNAL API
 */
private[cluster] object Reachability {
  val empty = new Reachability(Vector.empty, Map.empty)

  def apply(records: immutable.IndexedSeq[Record], versions: Map[UniqueAddress, Long]): Reachability =
    new Reachability(records, versions)

  def create(records: immutable.Seq[Record], versions: Map[UniqueAddress, Long]): Reachability = records match {
    case r: immutable.IndexedSeq[Record] ⇒ apply(r, versions)
    case _                               ⇒ apply(records.toVector, versions)
  }

  @SerialVersionUID(1L)
  case class Record(observer: UniqueAddress, subject: UniqueAddress, status: ReachabilityStatus, version: Long)

  sealed trait ReachabilityStatus
  @SerialVersionUID(1L) case object Reachable extends ReachabilityStatus
  @SerialVersionUID(1L) case object Unreachable extends ReachabilityStatus
  @SerialVersionUID(1L) case object Terminated extends ReachabilityStatus

}

/**
 * INTERNAL API
 *
 * Immutable data structure that holds the reachability status of subject nodes as seen
 * from observer nodes. Failure detector for the subject nodes exist on the
 * observer nodes. Changes (reachable, unreachable, terminated) are only performed
 * by observer nodes to its own records. Each change bumps the version number of the
 * record, and thereby it is always possible to determine which record is newest when
 * merging two instances.
 *
 * Aggregated status of a subject node is defined as (in this order):
 * - Terminated if any observer node considers it as Terminated
 * - Unreachable if any observer node considers it as Unreachable
 * - Reachable otherwise, i.e. no observer node considers it as Unreachable
 */
@SerialVersionUID(1L)
private[cluster] class Reachability private (
  val records: immutable.IndexedSeq[Reachability.Record],
  val versions: Map[UniqueAddress, Long]) extends Serializable {

  import Reachability._

  // lookup cache with observer as key, and records by subject as value
  @transient private lazy val table: Map[UniqueAddress, Map[UniqueAddress, Reachability.Record]] =
    records.groupBy(_.observer).map {
      case (observer, observerRows) ⇒
        val observerMap: Map[UniqueAddress, Record] =
          (observerRows.map { row ⇒ (row.subject -> row) })(breakOut)
        (observer -> observerMap)
    }

  @transient private lazy val aggregatedStatus: Map[UniqueAddress, Reachability.ReachabilityStatus] = {
    val subjects: Set[UniqueAddress] = records.map(_.subject)(breakOut)
    subjects.collect { case subject ⇒ subject -> aggregateStatus(subject, records.iterator) }(breakOut)
  }

  @tailrec private def aggregateStatus(subject: UniqueAddress, records: Iterator[Record],
                                       foundUnreachable: Boolean = false): ReachabilityStatus = {
    if (records.hasNext) {
      val r = records.next()
      if (r.subject != subject) aggregateStatus(subject, records, foundUnreachable)
      else if (r.status == Terminated) r.status
      else if (r.status == Unreachable) aggregateStatus(subject, records, true)
      else aggregateStatus(subject, records, foundUnreachable)
    } else {
      if (foundUnreachable) Unreachable else Reachable
    }
  }

  def unreachable(observer: UniqueAddress, subject: UniqueAddress): Reachability =
    change(observer, subject, Unreachable)

  def reachable(observer: UniqueAddress, subject: UniqueAddress): Reachability =
    change(observer, subject, Reachable)

  def terminated(observer: UniqueAddress, subject: UniqueAddress): Reachability =
    change(observer, subject, Terminated)

  private def currentVersion(observer: UniqueAddress): Long = versions.get(observer) match {
    case Some(v) ⇒ v
    case None    ⇒ 0
  }

  private def nextVersion(observer: UniqueAddress): Long = currentVersion(observer) + 1

  private def change(observer: UniqueAddress, subject: UniqueAddress, status: ReachabilityStatus): Reachability = {
    val v = nextVersion(observer)
    val newVersions = versions.updated(observer, v)
    val newRecord = Record(observer, subject, status, v)
    table.get(observer) match {
      case None if status == Reachable ⇒ this
      case None ⇒
        new Reachability(records :+ newRecord, newVersions)

      case Some(oldObserverRows) ⇒

        oldObserverRows.get(subject) match {
          case None ⇒
            if (status == Reachable && oldObserverRows.forall { case (_, r) ⇒ r.status == Reachable }) {
              // all Reachable, prune by removing the records of the observer, and bump the version
              new Reachability(records.filterNot(_.observer == observer), newVersions)
            } else
              new Reachability(records :+ newRecord, newVersions)
          case Some(oldRecord) ⇒
            if (oldRecord.status == Terminated || oldRecord.status == status)
              this
            else {
              if (status == Reachable && oldObserverRows.forall { case (_, r) ⇒ r.status == Reachable || r.subject == subject }) {
                // all Reachable, prune by removing the records of the observer, and bump the version
                new Reachability(records.filterNot(_.observer == observer), newVersions)
              } else {
                val newRecords = records.updated(records.indexOf(oldRecord), newRecord)
                new Reachability(newRecords, newVersions)
              }
            }
        }
    }
  }

  def merge(allowed: immutable.Set[UniqueAddress], other: Reachability): Reachability = {
    val recordBuilder = new immutable.VectorBuilder[Record]
    val versionsBuilder = new scala.collection.mutable.MapBuilder[UniqueAddress, Long, Map[UniqueAddress, Long]](Map.empty)
    allowed foreach { observer ⇒
      val observerVersion1 = this.currentVersion(observer)
      val observerVersion2 = other.currentVersion(observer)
      (this.table.get(observer), other.table.get(observer)) match {
        case (None, None) ⇒
        case (Some(rows1), Some(rows2)) ⇒
          mergeObserverRows(rows1, rows2, observerVersion1, observerVersion2, recordBuilder)
        case (Some(rows1), None) ⇒
          recordBuilder ++= rows1.collect { case (_, r) if r.version > observerVersion2 ⇒ r }
        case (None, Some(rows2)) ⇒
          recordBuilder ++= rows2.collect { case (_, r) if r.version > observerVersion1 ⇒ r }
      }
      (observerVersion1, observerVersion2) match {
        case (0, 0)   ⇒
        case (v1, v2) ⇒ versionsBuilder += ((observer, if (v1 > v2) v1 else v2))
      }
    }

    new Reachability(recordBuilder.result(), versionsBuilder.result())
  }

  private def mergeObserverRows(
    rows1: Map[UniqueAddress, Reachability.Record], rows2: Map[UniqueAddress, Reachability.Record],
    observerVersion1: Long, observerVersion2: Long,
    recordBuilder: immutable.VectorBuilder[Record]): Unit = {

    val allSubjects = rows1.keySet ++ rows2.keySet
    allSubjects foreach { subject ⇒
      (rows1.get(subject), rows2.get(subject)) match {
        case (Some(r1), Some(r2)) ⇒
          recordBuilder += (if (r1.version > r2.version) r1 else r2)
        case (Some(r1), None) ⇒
          if (r1.version > observerVersion2)
            recordBuilder += r1
        case (None, Some(r2)) ⇒
          if (r2.version > observerVersion1)
            recordBuilder += r2
        case (None, None) ⇒
          throw new IllegalStateException(s"Unexpected [$subject]")
      }
    }
  }

  def remove(nodes: Iterable[UniqueAddress]): Reachability = {
    val nodesSet = nodes.to[immutable.HashSet]
    val newRecords = records.filterNot(r ⇒ nodesSet(r.observer) || nodesSet(r.subject))
    if (newRecords.size == records.size) this
    else {
      val newVersions = versions -- nodes
      Reachability(newRecords, newVersions)
    }
  }

  def status(observer: UniqueAddress, subject: UniqueAddress): ReachabilityStatus =
    table.get(observer) match {
      case None ⇒ Reachable
      case Some(observerRows) ⇒ observerRows.get(subject) match {
        case None         ⇒ Reachable
        case Some(record) ⇒ record.status
      }
    }

  def status(node: UniqueAddress): ReachabilityStatus =
    aggregatedStatus.getOrElse(node, Reachable)

  def isReachable(node: UniqueAddress): Boolean =
    aggregatedStatus.get(node) match {
      case None            ⇒ true
      case Some(Reachable) ⇒ true
      case _               ⇒ false
    }

  def isReachable(observer: UniqueAddress, subject: UniqueAddress): Boolean =
    status(observer, subject) == Reachable

  def isAllReachable: Boolean = aggregatedStatus.isEmpty

  /**
   * Doesn't include terminated
   */
  def allUnreachable: Set[UniqueAddress] = aggregatedStatus.collect {
    case (subject, status) if status == Unreachable ⇒ subject
  }(breakOut)

  def allUnreachableOrTerminated: Set[UniqueAddress] = aggregatedStatus.collect {
    case (subject, status) if status == Unreachable || status == Terminated ⇒ subject
  }(breakOut)

  /**
   * Doesn't include terminated
   */
  def allUnreachableFrom(observer: UniqueAddress): Set[UniqueAddress] =
    table.get(observer) match {
      case None ⇒ Set.empty
      case Some(observerRows) ⇒
        observerRows.collect {
          case (subject, record) if record.status == Unreachable ⇒ subject
        }(breakOut)
    }

  def observersGroupedByUnreachable: Map[UniqueAddress, Set[UniqueAddress]] = {
    records.groupBy(_.subject).collect {
      case (subject, records) if records.exists(_.status == Unreachable) ⇒
        val observers: Set[UniqueAddress] =
          records.collect { case r if r.status == Unreachable ⇒ r.observer }(breakOut)
        (subject -> observers)
    }
  }

  def allObservers: Set[UniqueAddress] = versions.keySet

  def allSubjects: Set[UniqueAddress] = aggregatedStatus.keySet

  def recordsFrom(observer: UniqueAddress): immutable.IndexedSeq[Record] = {
    table.get(observer) match {
      case None               ⇒ Vector.empty
      case Some(observerRows) ⇒ observerRows.valuesIterator.toVector
    }
  }

  override def hashCode: Int = versions.hashCode

  override def equals(obj: Any): Boolean = obj match {
    case other: Reachability ⇒
      records.size == other.records.size && versions == versions && table == other.table
    case _ ⇒ false
  }

  override def toString: String = {
    val rows = for {
      observer ← table.keys.toSeq.sorted
      observerRows = table(observer)
      subject ← observerRows.keys.toSeq.sorted
    } yield {
      val record = observerRows(subject)
      val aggregated = aggregatedStatus.get(subject).getOrElse("")
      s"${observer.address} -> ${subject.address}: ${record.status} [$aggregated] (${record.version})"
    }

    rows.mkString(", ")
  }

}

