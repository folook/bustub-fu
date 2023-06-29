//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      cur_(nullptr, {}, nullptr) {}

void SeqScanExecutor::Init() {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_heap_ = table_info->table_.get();
  cur_ = table_heap_->Begin(exec_ctx_->GetTransaction());
  LockTable();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_ != table_heap_->End()) {
    *rid = cur_->GetRid();
    LockRow(*rid);
    *tuple = *cur_;
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
        exec_ctx_->GetLockManager() != nullptr) {
      exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(),
                                             exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->oid_, *rid);
    }
    (cur_)++;
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

void SeqScanExecutor::LockTable() {
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &oid = plan_->table_oid_;
  bool res = true;

  try {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      res = lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, oid);
    }
    if (!res) {
      assert(txn->GetState() == TransactionState::ABORTED);
      throw ExecutionException("SeqScanExecutor::Init() lock fail");
    }
  } catch (TransactionAbortException &e) {
    assert(txn->GetState() == TransactionState::ABORTED);
    throw ExecutionException("SeqScanExecutor::Init() lock fail");
  }
}

void SeqScanExecutor::LockRow(const RID &rid) {
  const auto &txn = exec_ctx_->GetTransaction();
  const auto &lock_mgr = exec_ctx_->GetLockManager();
  const auto &oid = plan_->GetTableOid();
  bool res = true;
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!txn->IsRowExclusiveLocked(oid, rid)) {
      res = lock_mgr->LockRow(txn, LockManager::LockMode::SHARED, oid, rid);
    }
  }
  if (!res) {
    txn->SetState(TransactionState::ABORTED);
    throw ExecutionException("SeqScanExecutor::Next() lock fail");
  }
}
}  // namespace bustub
