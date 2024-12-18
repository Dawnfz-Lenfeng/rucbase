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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle* fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager* sm_manager_;

   public:
    UpdateExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context* context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        // // For each record that needs to be updated
        // for (auto& rid : rids_) {
        //     // 1. Get the old record
        //     RmRecord old_rec = fh_->get_record(rid, context_);

        //     // 2. Create new record with updated values
        //     RmRecord new_rec(fh_->get_file_hdr().record_size);
        //     memcpy(new_rec.data, old_rec.data, fh_->get_file_hdr().record_size);

        //     // Apply updates according to set_clauses_
        //     for (const auto& clause : set_clauses_) {
        //         auto& col = tab_.get_col(clause.col_name);
        //         // Type check
        //         if (col.type != clause.val.type) {
        //             throw IncompatibleTypeError(coltype2str(col.type), coltype2str(clause.val.type));
        //         }
        //         // Update value
        //         clause.val.init_raw(col.len);
        //         memcpy(new_rec.data + col.offset, clause.val.raw->data, col.len);
        //     }

        //     // 3. Update indexes if necessary
        //     for (auto& index : tab_.indexes) {
        //         // Check if any updated column is part of this index
        //         bool need_update_index = false;
        //         for (const auto& clause : set_clauses_) {
        //             for (const auto& idx_col : index.cols) {
        //                 if (clause.col_name == idx_col.name) {
        //                     need_update_index = true;
        //                     break;
        //                 }
        //             }
        //             if (need_update_index) break;
        //         }

        //         // Only update index if affected columns are modified
        //         if (need_update_index) {
        //             auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols))
        //                           .get();

        //             // Delete old key
        //             std::vector<char> old_key(index.col_tot_len);
        //             int offset = 0;
        //             for (size_t i = 0; i < index.col_num; ++i) {
        //                 memcpy(old_key.data() + offset, old_rec.data + index.cols[i].offset, index.cols[i].len);
        //                 offset += index.cols[i].len;
        //             }
        //             ih->delete_entry(old_key.data(), context_->txn_);

        //             // Insert new key
        //             std::vector<char> new_key(index.col_tot_len);
        //             offset = 0;
        //             for (size_t i = 0; i < index.col_num; ++i) {
        //                 memcpy(new_key.data() + offset, new_rec.data + index.cols[i].offset, index.cols[i].len);
        //                 offset += index.cols[i].len;
        //             }
        //             ih->insert_entry(new_key.data(), rid, context_->txn_);
        //         }
        //     }

        //     // 4. Update the record in table file
        //     fh_->update_record(rid, new_rec.data, context_);
        // }

        return nullptr;
    }

    Rid& rid() override { return _abstract_rid; }
};