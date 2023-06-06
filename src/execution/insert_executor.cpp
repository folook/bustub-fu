//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
  table_name_ = table_info_->name_;
  iterator_ = std::make_unique<TableIterator>(table_heap_->Begin(exec_ctx_->GetTransaction()));
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (successful_) {
    return false;
  }
  int count = 0;
  // 向 table_heap_ 中插入，然后需要向 index 中插入
  //  indexs 中插入的 tuple 和原本的 tuple 不同，需要调用 KeyFromTuple 构造
  while (child_executor_->Next(tuple, rid)) {
    if (table_heap_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
      auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_name_);
      for (auto index : indexs) {
        auto key = (*tuple).KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
      }
      //有多少行受到了影响
      count++;
    }
  }
  std::vector<Value> value;
  value.emplace_back(INTEGER, count);
  Schema schema(plan_->OutputSchema());
  *tuple = Tuple(value, &schema);
  successful_ = true;
  return true;

}

}  // namespace bustub
