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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  //这句已经把我的 cpu 烧了，有以下问题：
  /*
   * 1. exec_ctx_这个上下文是哪来的？为什么会有 GetCatalog 方法
   * 2. plan_哪来的，为什么会有GetTableOid方法？
   */
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  //初始化 table_heap 和 iterator
  tableHeap = table_info->table_.get();
  iterator_ = std::make_unique<TableIterator>(tableHeap->Begin(exec_ctx_->GetTransaction()));
}

//获得下一条 tuple盒 rid
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(*iterator_ != tableHeap->End()) {
    //*((*iterator_)++)什么意思？
    *tuple = *((*iterator_)++);
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

}  // namespace bustub
