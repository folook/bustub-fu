//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(std::move(left_executor)), right_child_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  //把右边的 tuple 缓存起来
  Tuple tuple;
  RID rid;
  left_child_->Init();
  right_child_->Init();
  while(right_child_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }

}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {

  /*总的属性是两个表的属性水平拼接起来*/
  std::vector<Column> columns(left_child_->GetOutputSchema().GetColumns());
  for(const auto& column : right_child_->GetOutputSchema().GetColumns()) {
    columns.push_back(column);
  }
  Schema join_schema(columns);

  if(plan_->GetJoinType() == JoinType::LEFT) {
    return LeftJoin(join_schema, tuple);
  } else if(plan_->GetJoinType() == JoinType::INNER) {
    return InnerJoin(join_schema, tuple);

  } else {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan_->GetJoinType()));
  }
}

auto NestedLoopJoinExecutor::InnerJoin(const Schema &schema, Tuple *tuple) -> bool {


  if (index_ != 0) {
    /*注意是从index开始*/
    for (uint32_t j = index_; j < right_tuples_.size(); j++) {
      index_ = (index_ + 1) % right_tuples_.size();
      if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_child_->GetOutputSchema(), &right_tuples_[j], right_child_->GetOutputSchema()).GetAs<bool>()) {
        std::vector<Value> values;
        for(uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(left_tuple_.GetValue(&(left_child_->GetOutputSchema()), i));
        }
        for(uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(right_tuples_[j].GetValue(&(right_child_->GetOutputSchema()), i));
        }
        *tuple = {values, &schema};
        return true;
      }
    }
  }

  //index_是右表的游标
  if(index_ == 0) {
    //遍历左边
    while(left_child_->Next(&left_tuple_, &left_rid_)) {
      //遍历右表
      for(Tuple right_tuple : right_tuples_) {
        //当前正在遍历第 0 条，下一条应该遍历第 1 条
        index_ = (index_ + 1) % right_tuples_.size();
        if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_child_->GetOutputSchema(), &right_tuple, right_child_->GetOutputSchema()).GetAs<bool>()) {
          //用来构造最终输出的 join 后的 tuple
          std::vector<Value> values;
          for(uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
            values.push_back(left_tuple_.GetValue(&(left_child_->GetOutputSchema()), i));
          }
          for(uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
            values.push_back(right_tuple.GetValue(&(right_child_->GetOutputSchema()), i));
          }
          *tuple = {values, &schema};
          return true;
        }
      }
    }

    //走到这里说明 左表 遍历完毕
    return false;

  }
  return  false;
}

auto NestedLoopJoinExecutor::LeftJoin(const Schema &schema, Tuple *tuple) -> bool {


  if (index_ != 0) {
    /*注意是从index开始*/
    for (uint32_t j = index_; j < right_tuples_.size(); j++) {
      index_ = (index_ + 1) % right_tuples_.size();
      if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_child_->GetOutputSchema(), &right_tuples_[j], right_child_->GetOutputSchema()).GetAs<bool>()) {
        is_match_ = true;
        std::vector<Value> values;
        for(uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(left_tuple_.GetValue(&(left_child_->GetOutputSchema()), i));
        }
        for(uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(right_tuples_[j].GetValue(&(right_child_->GetOutputSchema()), i));
        }
        *tuple = {values, &schema};
        return true;
      }
    }
  }

  //index_是右表的游标
  if(index_ == 0) {
    //遍历左边
    while(left_child_->Next(&left_tuple_, &left_rid_)) {
      is_match_ = false;
      //遍历右表
      for(Tuple right_tuple : right_tuples_) {
        //当前正在遍历第 0 条，下一条应该遍历第 1 条
        index_ = (index_ + 1) % right_tuples_.size();
        if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_child_->GetOutputSchema(), &right_tuple, right_child_->GetOutputSchema()).GetAs<bool>()) {
          is_match_ = true;
          //用来构造最终输出的 join 后的 tuple
          std::vector<Value> values;
          for(uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
            values.push_back(left_tuple_.GetValue(&(left_child_->GetOutputSchema()), i));
          }
          for(uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
            values.push_back(right_tuple.GetValue(&(right_child_->GetOutputSchema()), i));
          }
          *tuple = {values, &schema};
          return true;
        }
      }
      /*遍历完右表都没有匹配的情况*/
      /*如果跟右边没有任何一行能匹配，则需要构造一个空tuple来join*/
      if(!is_match_) {
        std::vector<Value> values;
        for(uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(left_tuple_.GetValue(&(left_child_->GetOutputSchema()), i));
        }
        for(uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = {values, &schema};
        //记得要修改匹配 flag 为 true，这个 flag 表示左行已经匹配了一个空行或者右行
        is_match_ = true;
        return true;
      }
    }

    //执行到这里，说明左表遍历完毕
    return false;

  }
  return false;
}

}  // namespace bustub


