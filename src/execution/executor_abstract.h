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
#include "common/common.h"
#include "execution_defs.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor {
   public:
    Rid _abstract_rid;

    Context *context_;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple(){};

    virtual void nextTuple(){};

    virtual bool is_end() const { return true; };

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta(); };

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

   protected:
    template <typename F>
    bool eval_conds(const std::vector<Condition> &conds, F &&get_values) {
        for (auto &cond : conds) {
            auto [lhs, rhs, type, len] = std::forward<F>(get_values)(cond);
            if (!eval_cond(lhs, rhs, type, len, cond.op)) {
                return false;
            }
        }
        return true;
    }

    char *get_value(const std::vector<ColMeta> &rec_cols, const TabCol &target, const RmRecord *rec) {
        auto pos = get_col(rec_cols, target);
        return rec->data + pos->offset;
    }

   private:
    bool eval_cond(const char *lhs, const char *rhs, ColType type, int len, const CompOp op) {
        int cmp = ix_compare(lhs, rhs, type, len);
        switch (op) {
            case OP_EQ:
                return cmp == 0;
            case OP_NE:
                return cmp != 0;
            case OP_LT:
                return cmp < 0;
            case OP_GT:
                return cmp > 0;
            case OP_LE:
                return cmp <= 0;
            case OP_GE:
                return cmp >= 0;
            default:
                throw InternalError("Unexpected op type");
        }
    }
};