//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  index_tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  iterator_ = index_tree_->GetBeginIterator();
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_)->table_.get();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ != index_tree_->GetEndIterator()) {
    *rid = (*iterator_).second;
    if (table_heap_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction())) {
      iterator_.operator++();
      return true;
    }
  }
  return false;
}
}  // namespace bustub
