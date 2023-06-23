//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /** Whether the lock has been granted or not */
    bool granted_{false};
  };

  class LockRequestQueue {
   public:
    /** List of lock requests for the same resource (table or row) */
    std::list<std::shared_ptr<LockRequest>> request_queue_;
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    std::mutex latch_;
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
  }

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *
   *    LockTable() 和 LockRow()都是阻塞方法，应该等待（阻塞）直到锁被授予才可以 return
   *    如果阻塞期间事务被 abort，则这两个方法不会被授予锁并且返回 false
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *
   *    LM 为每个 表/tuple 维护一个队列，锁应该以先进先出的方式被授予给事务（哪个事务先申请，哪个事务先获得）
   *    如果有多个锁兼容的请求，在遵循先进先出的大前提下，应该同时给这些事务授予锁
   *
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *
   *    表锁支持所有的锁模式：S 锁、X 锁、意向锁
   *    行锁不应该支持意向锁，尝试给 行 申请意向锁的事务应该被abort，并且抛出异常
   *
   *
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   *    根据 DBMS 的用户设置的隔离级别，一个事务应该在满足以下条件时才能尝试申请锁：
   *    - 仅当这个事务需要时
   *    - 仅当这个事务被允许申请对应类型的锁时
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *
   *    todo 比如，在 读未提交 隔离级别下不需要 S/IS/SI 锁，所以任何事务都不应该尝试获取 S/IS/SI 锁。如果有这种情况发生，就要中止事务且抛出异常
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *
   *    todo 类似地， 事务在 SHRINKING 状态时不允许对行申请 X/IX 锁，任何这种操作都要中止事务且抛出异常
   *    todo 下面就详细告诉你，在每一种隔离级别下，哪些锁可以加，哪些锁不可以加
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *
   *        可重复读 RR：
   *        -事务应该申请所有的锁
   *        -在 growing 阶段，所有的锁都可以被申请
   *        -在 shrinking 阶段，不允许申请任何锁
   *
   *
   *    READ_COMMITTED:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *
   *        读提交 RC
   *        -事务应该申请所有的锁
   *        -在 growing 阶段，所有的锁都可以被申请
   *        -在 shrinking 阶段，仅允许申请 IS 、S 锁
   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *
   *        读未提交 RU
   *        -事务 仅可以申请 X、IX 锁
   *        -在 growing 阶段，仅可以申请 X、IX 锁
   *        - S, IS, SIX locks 永远不允许
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *
   * 多级锁定
   *    当申请行锁时，Lock()函数应该确保事务在行的父节点（表）上已经有了相应的锁，比如，如果事务为一个行申请排他锁，那么这个事务必须先在表（父节点）上
   *    拥有 X、IX、SIX 锁，如果没有，Lock()函数就要中止事务并且抛出异常
   *
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *
   * 锁升级
   *    针对某个<已经加了锁的>资源申请锁（针对这个资源调用 Lock()函数），应该遵循以下规则：
   *    - 如果请求的锁类型和已有的锁相同，则 Lock() should return true；
   *    - 如果要加的锁类型和<已有的锁>类型不同，Lock()函数应该对锁进行升级
   *
   *    A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
   *    关键：正在升级的锁请求，优先于其他事务的正在等待的锁请求，即一旦这个资源的这个锁被释放，升级锁这个锁请求会被立马授权锁（第一个等待的位置）
   *    即：升级的锁的优先级是很高的
   *
   *    在升级期间，仅允许以下转换：
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *    其他所有升级都被认为是非法，主张这种升级的中止且抛出异常
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *    另外，在给定资源上，至允许有一个事务升级锁，如果发现同一资源上有多个升级的事务，应该设置这些并发的事务中止且抛出异常
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   *    如果将锁授予事务，则锁管理器应该更新这个事务的锁集(请检查Transaction.h)
   *    注意这里不是锁表，锁表是以资源位索引（key），value 记录了这个针对资源的各个事务的请求
   *    而这里的锁集是以事务为索引，记录了这个事务目前拿到了几把锁，记录的目的是，如果这个事务脏哦g内hi了，就可以把事务对应的锁资源全部释放
   *
   *
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

  auto GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                 const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool;

  auto InsertOrDeleteTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request, bool insert)
      -> void;

  auto InsertOrDeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request, bool insert)
      -> void;

  auto InsertRowLockSet(const std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> &lock_set,
                        const table_oid_t &oid, const RID &rid) -> void {
    auto row_lock_set = lock_set->find(oid);
    if (row_lock_set == lock_set->end()) {
      lock_set->emplace(oid, std::unordered_set<RID>{});
      row_lock_set = lock_set->find(oid);
    }
    row_lock_set->second.emplace(rid);
  }

  auto DeleteRowLockSet(const std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> &lock_set,
                        const table_oid_t &oid, const RID &rid) -> void {
    auto row_lock_set = lock_set->find(oid);
    if (row_lock_set == lock_set->end()) {
      return;
    }
    row_lock_set->second.erase(rid);
  }

  auto Dfs(txn_id_t txn_id) -> bool {
    if (safe_set_.find(txn_id) != safe_set_.end()) {
      return false;
    }
    active_set_.insert(txn_id);

    std::vector<txn_id_t> &next_node_vector = waits_for_[txn_id];
    std::sort(next_node_vector.begin(), next_node_vector.end());
    for (txn_id_t const next_node : next_node_vector) {
      if (active_set_.find(next_node) != active_set_.end()) {
        return true;
      }
      if (Dfs(next_node)) {
        return true;
      }
    }

    active_set_.erase(txn_id);
    safe_set_.insert(txn_id);
    return false;
  }

  auto DeleteNode(txn_id_t txn_id) -> void;

 private:
  /** Fall 2022 */
  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;

  std::set<txn_id_t> safe_set_;
  std::set<txn_id_t> txn_set_;
  std::unordered_set<txn_id_t> active_set_;

  std::unordered_map<txn_id_t, RID> map_txn_rid_;
  std::unordered_map<txn_id_t, table_oid_t> map_txn_oid_;
};

}  // namespace bustub
