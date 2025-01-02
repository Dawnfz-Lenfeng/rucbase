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

#include <condition_variable>
#include <mutex>
#include <index/ix_defs.h>
#include "transaction/transaction.h"

static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "SIX", "X"};

class GapLock {
   public:
    GapLock(txn_id_t txn_id, const Iid& start_rid, const Iid& end_rid)
        : txn_id_(txn_id), start_rid_(start_rid), end_rid_(end_rid) {}

    txn_id_t txn_id_;  // 持有锁的事务ID
    Iid start_rid_;    // 间隙起始位置
    Iid end_rid_;      // 间隙结束位置
};

// 间隙锁表
class GapLockTable {
   public:
    // 尝试在区间上加锁
    bool lock_gap(Transaction* txn, int tab_fd, const Iid& start_rid, const Iid& end_rid);

    // 检查是否与已有的间隙锁冲突
    bool check_gap_conflict(int tab_fd, const Iid& rid);

    // 释放事务持有的所有间隙锁
    void release_gap_locks(txn_id_t txn_id);

   private:
    std::mutex latch_;  // 保护gap_locks_的并发访问
    // 每个表的间隙锁列表: <tab_fd, vector<GapLock>>
    std::unordered_map<int, std::vector<GapLock>> gap_locks_;
};

class LockManager {
    /* 加锁类型，包括共享锁、排他锁、意向共享锁、意向排他锁、SIX（意向排他锁+共享锁） */
    enum class LockMode { INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED, S_IX, EXLUCSIVE };

    /* 用于标识加锁队列中排他性最强的锁类型，例如加锁队列中有SHARED和EXLUSIVE两个加锁操作，则该队列的锁模式为X */
    enum class GroupLockMode { NON_LOCK, IS, IX, S, SIX, X };

    /* 事务的加锁申请 */
    class LockRequest {
       public:
        LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

        txn_id_t txn_id_;     // 申请加锁的事务ID
        LockMode lock_mode_;  // 事务申请加锁的类型
        bool granted_;        // 该事务是否已经被赋予锁

        bool update_lock_mode(LockMode& lock_mode);
    };

    /* 数据项上的加锁队列 */
    class LockRequestQueue {
       public:
        std::list<LockRequest> request_queue_;  // 加锁队列
        std::condition_variable cv_;  // 条件变量，用于唤醒正在等待加锁的申请，在no-wait策略下无需使用
        GroupLockMode group_lock_mode_ = GroupLockMode::NON_LOCK;  // 加锁队列的锁模式

        void push_back(LockDataId lock_data_id, LockMode lock_mode, Transaction* txn);
        void erase(txn_id_t txn_id);
        bool check_conflict(GroupLockMode group_lock_mode, LockDataType lock_data_type);
    };

   public:
    LockManager() {}

    ~LockManager() {}

    void lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    void lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    void lock_shared_on_table(Transaction* txn, int tab_fd);

    void lock_exclusive_on_table(Transaction* txn, int tab_fd);

    void lock_IS_on_table(Transaction* txn, int tab_fd);

    void lock_IX_on_table(Transaction* txn, int tab_fd);

    void unlock(Transaction* txn, LockDataId lock_data_id);

    // 加间隙锁
    void lock_gap(Transaction* txn, int tab_fd, const Iid& start_rid, const Iid& end_rid);

    // 检查间隙锁冲突
    void check_gap_conflict(Transaction* txn, int tab_fd, const Rid& rid);

    // 释放事务的所有间隙锁
    void release_gap_locks(txn_id_t txn_id) {
        gap_lock_table_.release_gap_locks(txn_id);
    }

   private:
    std::mutex latch_;                                             // 用于锁表的并发
    std::unordered_map<LockDataId, LockRequestQueue> lock_table_;  // 全局锁表
    GapLockTable gap_lock_table_;                                  // 间隙锁表

    void lock_on_record(Transaction* txn, const Rid& rid, int tab_fd, LockMode lock_mode);
    void lock_on_table(Transaction* txn, int tab_fd, LockMode lock_mode);
    static GroupLockMode get_group_lock_mode(LockMode lock_mode);
};
