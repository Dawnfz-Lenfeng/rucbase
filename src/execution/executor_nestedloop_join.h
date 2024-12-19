/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;   // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;  // 右儿子节点（需要join的表）
    size_t len_;                               // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                // join后获得的记录的字段

    std::vector<Condition> fed_conds_;    // join条件
    std::unique_ptr<RmRecord> left_rec_;  // 左表当前记录
    bool isend;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);
    }

    void beginTuple() override {
        left_->beginTuple();
        if (left_->is_end()) {
            isend = true;
            return;
        }
        left_rec_ = left_->Next();
        right_->beginTuple();
    }

    void nextTuple() override {
        // 移动到下一个匹配的记录对
        while (!isend) {
            right_->nextTuple();
            if (right_->is_end()) {
                // 右表扫描完，移动左表到下一条记录
                left_->nextTuple();
                if (left_->is_end()) {
                    isend = true;
                    return;
                }
                left_rec_ = left_->Next();
                right_->beginTuple();
            } else {
                // 检查当前记录对是否满足连接条件
                auto right_rec = right_->Next();
                for (auto &cond : fed_conds_) {
                    auto [lhs, rhs, type, len] = get_compare_values(left_rec_.get(), right_rec.get(), cond);
                    if (eval_cond(lhs, rhs, type, len, cond.op)) {
                        return;  // 找到匹配的记录对
                    }
                }
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        auto right_rec = right_->Next();
        // 创建连接后的记录
        auto join_rec = std::make_unique<RmRecord>(len_);
        // 复制左表记录
        memcpy(join_rec->data, left_rec_->data, left_->tupleLen());
        // 复制右表记录
        memcpy(join_rec->data + left_->tupleLen(), right_rec->data, right_->tupleLen());
        return join_rec;
    }

    bool is_end() const override { return isend; }

    Rid &rid() override { return _abstract_rid; }

   private:
    std::tuple<char *, char *, ColType, int> get_compare_values(const RmRecord *left_rec, const RmRecord *right_rec,
                                                                const Condition &cond) {
        auto lhs_col = get_col(cols_, cond.lhs_col);
        auto rhs_col = get_col(cols_, cond.rhs_col);

        char *lhs_val = nullptr;
        char *rhs_val = nullptr;

        // 确定左值来自哪个表
        if (lhs_col->offset < left_->tupleLen()) {
            lhs_val = left_rec->data + lhs_col->offset;
        } else {
            lhs_val = right_rec->data + (lhs_col->offset - left_->tupleLen());
        }

        // 确定右值来自哪个表
        if (rhs_col->offset < left_->tupleLen()) {
            rhs_val = left_rec->data + rhs_col->offset;
        } else {
            rhs_val = right_rec->data + (rhs_col->offset - left_->tupleLen());
        }

        return {lhs_val, rhs_val, lhs_col->type, lhs_col->len};
    }
};