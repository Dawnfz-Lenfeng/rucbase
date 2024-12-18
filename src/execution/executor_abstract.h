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
    bool eval_conds(const std::vector<ColMeta> &cols, const RmRecord *rec, const std::vector<Condition> &conds) {
        for (const auto &cond : conds) {
            // 获取左值列的元数据
            auto lhs_col = get_col(cols, cond.lhs_col);

            // 从记录中获取左值数据
            char *lhs_data = rec->data + lhs_col->offset;
            Value lhs_val;

            // 根据列的类型设置左值
            switch (lhs_col->type) {
                case TYPE_INT:
                    lhs_val.set_int(*(int *)lhs_data);
                    break;
                case TYPE_FLOAT:
                    lhs_val.set_float(*(float *)lhs_data);
                    break;
                case TYPE_STRING:
                    lhs_val.set_str(std::string(lhs_data, lhs_col->len));
                    break;
                default:
                    throw InternalError("Unexpected column type");
            }

            if (cond.is_rhs_val) {
                // 右值为常量，直接与cond.rhs_val比较
                if (!evaluate_compare(lhs_val, cond.rhs_val, cond.op)) {
                    return false;
                }
            } else {
                // 右值为列，需要从记录中获取值
                auto rhs_col = get_col(cols, cond.rhs_col);
                char *rhs_data = rec->data + rhs_col->offset;
                Value rhs_val;

                // 根据列的类型设置右值
                switch (rhs_col->type) {
                    case TYPE_INT:
                        rhs_val.set_int(*(int *)rhs_data);
                        break;
                    case TYPE_FLOAT:
                        rhs_val.set_float(*(float *)rhs_data);
                        break;
                    case TYPE_STRING:
                        rhs_val.set_str(std::string(rhs_data, rhs_col->len));
                        break;
                    default:
                        throw InternalError("Unexpected column type");
                }

                // 比较左右值
                if (!evaluate_compare(lhs_val, rhs_val, cond.op)) {
                    return false;
                }
            }
        }
        return true;
    }

   private:
    bool evaluate_compare(const Value &lhs, const Value &rhs, CompOp op) {
        switch (op) {
            case OP_EQ:
                return lhs == rhs;
            case OP_NE:
                return lhs != rhs;
            case OP_LT:
                return lhs < rhs;
            case OP_GT:
                return lhs > rhs;
            case OP_LE:
                return lhs <= rhs;
            case OP_GE:
                return lhs >= rhs;
            default:
                throw InternalError("Unexpected compare type");
        }
    }
};