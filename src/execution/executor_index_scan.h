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

#include <climits>
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表名称
    TabMeta tab_;                       // 表的元数据
    std::vector<Condition> conds_;      // 扫描条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // 需要读取的字段
    size_t len_;                        // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;  // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;
    // 扫描范围结构
    struct ScanRange {
        char *lower_key;       // 范围下界
        char *upper_key;       // 范围上界
        bool lower_inclusive;  // 是否包含下界
        bool upper_inclusive;  // 是否包含上界

        ScanRange(char *lower, char *upper, bool l_incl, bool u_incl)
            : lower_key(lower), upper_key(upper), lower_inclusive(l_incl), upper_inclusive(u_incl) {}
    };
    std::vector<ScanRange> scan_ranges_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
                      std::vector<std::string> index_col_names, Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names;
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        auto ih =
            sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)).get();
        analyze_conditions();
        lock_ranges();
        scan_ = std::make_unique<IxScan>(ih, ih->leaf_begin(), ih->leaf_end(), sm_manager_->get_bpm());

        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(fed_conds_,
                           [this, rec = rec.get()](const Condition &cond) { return get_compare_values(rec, cond); })) {
                return;
            }
            scan_->next();
        }
    }

    void nextTuple() override {
        if (scan_->is_end()) {
            return;
        }

        for (scan_->next(); !scan_->is_end(); scan_->next()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(fed_conds_,
                           [this, rec = rec.get()](const Condition &cond) { return get_compare_values(rec, cond); })) {
                return;
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override { return fh_->get_record(rid_, context_); }

    Rid &rid() override { return rid_; }

    bool is_end() const override { return scan_->is_end(); }

   private:
    std::tuple<char *, char *, ColType, int> get_compare_values(const RmRecord *rec, const Condition &cond) {
        auto lhs_col = get_col(cols_, cond.lhs_col);
        char *lhs = rec->data + lhs_col->offset;

        char *rhs = cond.is_rhs_val ? cond.rhs_val.raw->data : get_value(cols_, cond.rhs_col, rec);
        return {lhs, rhs, lhs_col->type, lhs_col->len};
    }

    const std::vector<ColMeta> &cols() const override { return cols_; }
    size_t tupleLen() const override { return len_; }

    // 分析条件得到扫描范围
    void analyze_conditions() {
        for (const auto &cond : fed_conds_) {
            // 只处理索引第一列的条件
            if (cond.lhs_col.col_name == index_col_names_[0] && cond.is_rhs_val) {
                char *key = cond.rhs_val.raw->data;
                switch (cond.op) {
                    case OP_EQ:
                        scan_ranges_.emplace_back(key, key, true, true);
                        break;
                    case OP_LT:
                        scan_ranges_.emplace_back(nullptr, key, true, false);
                        break;
                    case OP_LE:
                        scan_ranges_.emplace_back(nullptr, key, true, true);
                        break;
                    case OP_GT:
                        scan_ranges_.emplace_back(key, nullptr, false, true);
                        break;
                    case OP_GE:
                        scan_ranges_.emplace_back(key, nullptr, true, true);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    // 对范围加锁
    void lock_ranges() {
        auto ih =
            sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)).get();

        for (const auto &range : scan_ranges_) {
            // 获取范围边界的rid
            Iid start_iid = Iid{-1, -1};
            Iid end_iid = Iid{INT_MAX, INT_MAX};

            // 获取下界rid
            if (range.lower_key != nullptr) {
                // 最小rid
                start_iid = ih->lower_bound(range.lower_key);
                auto scan = IxScan(ih, start_iid, ih->leaf_end(), sm_manager_->get_bpm());
                if (!scan.is_end()) {
                    start_iid = scan.iid();
                    if (!range.lower_inclusive) {
                        // 不包含下界,移到下一个
                        scan.next();
                        if (!scan.is_end()) {
                            start_iid = scan.iid();
                        }
                    }
                }
            }

            // 获取上界rid
            if (range.upper_key != nullptr) {
                end_iid = ih->lower_bound(range.upper_key);
                auto scan = IxScan(ih, ih->leaf_begin(), end_iid, sm_manager_->get_bpm());

                if(!scan.is_end()) {
                    end_iid = scan.iid();
                    if(!range.upper_inclusive) {
                        // 不包含上界,使用当前位置
                        end_iid = scan.iid();
                    } else {
                        // 包含上界,移到下一个
                        scan.next();
                        if(!scan.is_end()) {
                            end_iid = scan.iid();
                        }
                    }
                }
            }

            // 对范围加锁
            context_->lock_mgr_->lock_gap(context_->txn_, fh_->GetFd(), start_iid, end_iid);
        }
    }
};