/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

// 锁相容性矩阵
// |     | X | IX | S | IS | SIX |
// |-----|---|----|---|----| --- |
// | X   | 0 | 0  | 0 | 0  |  0  |
// | IX  | 0 | 1  | 0 | 1  |  0  |
// | S   | 0 | 0  | 1 | 1  |  0  |
// | IS  | 0 | 1  | 1 | 1  |  1  |
// | SIX | 0 | 0  | 0 | 1  |  0  |
// |-----|---|----|---|----| --- |

void LockManager::LockRequestQueue::push_back(LockDataId lock_data_id, LockMode lock_mode, Transaction* txn) {
    LockRequest lock_request(txn->get_transaction_id(), lock_mode);
    lock_request.granted_ = true;  // no-wait策略下立即授予锁

    request_queue_.push_back(lock_request);

    txn->get_lock_set()->insert(lock_data_id);
}

bool LockManager::LockRequestQueue::check_conflict(GroupLockMode group_lock_mode, LockDataType lock_data_type) {
    if (group_lock_mode_ == GroupLockMode::X) {
        return true;  // X锁与其他锁都不兼容
    }

    if (lock_data_type == LockDataType::TABLE) {
        switch (group_lock_mode) {
            case GroupLockMode::S:
                if (group_lock_mode_ != GroupLockMode::S && group_lock_mode_ != GroupLockMode::IS) {
                    return true;
                }
                break;
            case GroupLockMode::IX:
                if (group_lock_mode_ != GroupLockMode::IX && group_lock_mode_ != GroupLockMode::IS) {
                    return true;
                }
                break;
            case GroupLockMode::SIX:
                if (group_lock_mode_ != GroupLockMode::IS) {
                    return true;
                }
                break;
            default:
                break;
        }
    }

    return false;
}

LockManager::GroupLockMode LockManager::get_group_lock_mode(LockMode lock_mode) {
    switch (lock_mode) {
        case LockMode::INTENTION_SHARED:
            return GroupLockMode::IS;
        case LockMode::INTENTION_EXCLUSIVE:
            return GroupLockMode::IX;
        case LockMode::SHARED:
            return GroupLockMode::S;
        case LockMode::S_IX:
            return GroupLockMode::SIX;
        case LockMode::EXLUCSIVE:
            return GroupLockMode::X;
        default:
            return GroupLockMode::NON_LOCK;
    }
}

void LockManager::lock_on_record(Transaction* txn, const Rid& rid, int tab_fd, LockMode lock_mode) {
    // 先获取表的意向锁(IS/IX)
    GroupLockMode group_lock_mode = get_group_lock_mode(lock_mode);
    lock_on_table(txn, tab_fd, lock_mode, group_lock_mode);

    std::unique_lock<std::mutex> latch(latch_);

    // 检查当前事务是否已经获得该记录的锁
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    auto& request_queue = lock_table_[lock_data_id];

    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            if (req.lock_mode_ >= lock_mode) {
                return;  // 已经有足够强的锁
            }
            // 需要升级锁
            req.lock_mode_ = lock_mode;
            if (request_queue.group_lock_mode_ < group_lock_mode) {
                request_queue.group_lock_mode_ = group_lock_mode;
            }
            return;
        }
    }

    // 检查冲突
    if (request_queue.check_conflict(group_lock_mode, LockDataType::RECORD)) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    // 创建新的锁请求
    request_queue.push_back(lock_data_id, lock_mode, txn);

    // 更新锁模式
    if (request_queue.group_lock_mode_ < group_lock_mode) {
        request_queue.group_lock_mode_ = group_lock_mode;
    }
}

void LockManager::lock_on_table(Transaction* txn, int tab_fd, LockMode lock_mode, GroupLockMode group_lock_mode) {
    std::unique_lock<std::mutex> latch(latch_);

    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    auto& request_queue = lock_table_[lock_data_id];

    // 检查当前事务的锁
    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            if (req.lock_mode_ >= lock_mode) {
                return;  // 已经有足够强的锁
            }
            // 需要升级锁
            req.lock_mode_ = lock_mode;
            if (request_queue.group_lock_mode_ < group_lock_mode) {
                request_queue.group_lock_mode_ = group_lock_mode;
            }
            return;
        }
    }

    // 检查冲突
    if (request_queue.check_conflict(group_lock_mode, LockDataType::TABLE)) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    // 创建新的锁请求
    request_queue.push_back(lock_data_id, lock_mode, txn);

    // 更新锁模式
    if (request_queue.group_lock_mode_ < group_lock_mode) {
        request_queue.group_lock_mode_ = group_lock_mode;
    }
}

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
void LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    lock_on_record(txn, rid, tab_fd, LockMode::SHARED);
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
void LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    lock_on_record(txn, rid, tab_fd, LockMode::EXLUCSIVE);
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
void LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    lock_on_table(txn, tab_fd, LockMode::SHARED, GroupLockMode::S);
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
void LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    lock_on_table(txn, tab_fd, LockMode::EXLUCSIVE, GroupLockMode::X);
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
void LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    lock_on_table(txn, tab_fd, LockMode::INTENTION_SHARED, GroupLockMode::IS);
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
void LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    lock_on_table(txn, tab_fd, LockMode::INTENTION_EXCLUSIVE, GroupLockMode::IX);
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */

void LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> latch(latch_);
    auto& request_queue = lock_table_[lock_data_id];

    for (auto it = request_queue.request_queue_.begin(); it != request_queue.request_queue_.end(); ++it) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            request_queue.request_queue_.erase(it);

            // 需要更新队列的锁模式
            if (request_queue.request_queue_.empty()) {
                request_queue.group_lock_mode_ = GroupLockMode::NON_LOCK;
            } else {
                // 找到队列中最强的锁模式
                GroupLockMode strongest = GroupLockMode::NON_LOCK;
                for (const auto& req : request_queue.request_queue_) {
                    GroupLockMode mode = get_group_lock_mode(req.lock_mode_);
                    if (mode > strongest) {
                        strongest = mode;
                    }
                }
                request_queue.group_lock_mode_ = strongest;
            }
            break;
        }
    }

    // 从事务的锁集合中移除
    txn->get_lock_set()->erase(lock_data_id);
}
