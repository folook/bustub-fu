//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)),
      //hash_table的 key 是 expression，即表达式树，如 min(v1+v2-2),key vector 的一个元素就是v1+v2-3树，而 value vector 的一个元素就是 min
      hash_table_(plan->GetAggregates(), plan_->GetAggregateTypes()), ht_iterator_(hash_table_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  auto *tuple = new Tuple();
  auto *rid = new RID();
      //从子节点 seqscan 获取 tuple
  while(child_->Next(tuple, rid)) {
    auto aggregate_key = MakeAggregateKey(tuple);
    auto aggreage_value = MakeAggregateValue(tuple);

    hash_table_.InsertCombine(aggregate_key, aggreage_value);
  }
  ht_iterator_ = hash_table_.Begin(); // 插入后需要重新指定 iterator_ 因为哈希表是无序的

  }




auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Schema schema(plan_->OutputSchema());
  if(ht_iterator_ != hash_table_.End()) {
    /*
       *  每次获取hashmap中的一个key
       *  key_1 = 【上等仓，女】
       *  key_2 = 【下等仓，男】
       * */
    std::vector<Value> value(ht_iterator_.Key().group_bys_);
    /*先放分组，再放数据*/
    for (const auto &aggregate : ht_iterator_.Val().aggregates_) {
      value.push_back(aggregate);
    }
    *tuple = {value, &schema};
    ++ht_iterator_;
    success_ = true;
    return true;
  }

  /* 特殊处理空表的情况：当为空表，且想获得统计信息时，只有countstar返回0，其他情况返回无效null*/
  // 空表执行 select count(*) from t1;
  if (!success_) {
    /*跟迭代器无关的逻辑，只需要返回一次*/
    success_ = true;
    if (plan_->GetGroupBys().empty()) {
      std::vector<Value> value;
      for (auto aggregate : plan_->GetAggregateTypes()) {
        switch (aggregate) {
          case AggregationType::CountStarAggregate:
            value.push_back(ValueFactory::GetIntegerValue(0));
            break;
          case AggregationType::CountAggregate:
          case AggregationType::SumAggregate:
          case AggregationType::MinAggregate:
          case AggregationType::MaxAggregate:
            value.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
            break;
        }
      }
      *tuple = {value, &schema};
      success_ = true;
      return true;
    } else {
      return false;
    }
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
