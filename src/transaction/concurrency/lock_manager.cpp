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

bool LockManager::LockRequestQueue::check_compatible(GroupLockMode group_lock_mode, LockDataType lock_data_type) {
    if (group_lock_mode_ == GroupLockMode::X) {
        return true;  // X锁为最高优先级
    }
    bool is_compatible = false;

    if (group_lock_mode_ != GroupLockMode::NON_LOCK && lock_data_type == LockDataType::TABLE) {
        bool is_compatible = false;
        // IS < IX < S < SIX < X
        switch (group_lock_mode) {
            case GroupLockMode::IS:
                is_compatible = is_compatible || group_lock_mode_ == GroupLockMode::IS;
            case GroupLockMode::IX:
                is_compatible = is_compatible || group_lock_mode_ == GroupLockMode::IX;
            case GroupLockMode::S:
                is_compatible = is_compatible || group_lock_mode_ == GroupLockMode::S;
            case GroupLockMode::SIX:
                is_compatible = is_compatible || group_lock_mode_ == GroupLockMode::SIX;
            case GroupLockMode::X:
                is_compatible = is_compatible || group_lock_mode_ == GroupLockMode::X;
        }
    } else {  // RECORD
        is_compatible = is_compatible || group_lock_mode_ == GroupLockMode::S;
    }

    return is_compatible;
}

bool LockManager::LockRequest::check_compatible(LockMode lock_mode, LockDataType lock_data_type) {
    if (lock_mode_ == LockMode::EXLUCSIVE) {
        return true;
    }

    bool is_compatible = false;

    if (lock_data_type == LockDataType::TABLE) {
        // IS < IX < S < SIX < X
        switch (lock_mode) {
            case LockMode::INTENTION_SHARED:
                is_compatible = is_compatible || lock_mode_ == LockMode::INTENTION_SHARED;
            case LockMode::INTENTION_EXCLUSIVE:
                is_compatible = is_compatible || lock_mode_ == LockMode::INTENTION_EXCLUSIVE;
            case LockMode::SHARED:
                is_compatible = is_compatible || lock_mode_ == LockMode::SHARED;
            case LockMode::S_IX:
                is_compatible = is_compatible || lock_mode_ == LockMode::S_IX;
        }
    } else {  // RECORD
        is_compatible = is_compatible || lock_mode_ == LockMode::SHARED;
    }

    return is_compatible;
}

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    // 先获取表的意向共享锁(IS)
    if (!lock_IS_on_table(txn, tab_fd)) {
        return false;
    }

    std::unique_lock<std::mutex> latch(latch_);

    // 检查当前事务是否已经获得该记录的锁
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    auto& request_queue = lock_table_[lock_data_id];

    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id() && req.check_compatible(LockMode::SHARED, LockDataType::RECORD)) {
            return true;
        }
    }

    // 检查其他请求是否与当前请求冲突
    if (request_queue.check_conflict(GroupLockMode::S, LockDataType::RECORD)) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    // 没有则创建新的锁请求
    request_queue.push_back(lock_data_id, LockMode::SHARED, txn);

    // 更新请求队列中的锁模式
    if (request_queue.check_compatible(GroupLockMode::S, LockDataType::RECORD)) {
        request_queue.group_lock_mode_ = GroupLockMode::S;
    }

    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) { return true; }

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> latch(latch_);

    // 检查当前事务是否已经获得该表的锁
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    auto& request_queue = lock_table_[lock_data_id];

    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id() && req.check_compatible(LockMode::SHARED, LockDataType::TABLE)) {
            return true;
        }
    }

    // 检查其他请求是否与当前请求冲突
    if (request_queue.check_conflict(GroupLockMode::S, LockDataType::TABLE)) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    // 没有则创建新的锁请求
    request_queue.push_back(lock_data_id, LockMode::SHARED, txn);

    // 更新请求队列中的锁模式
    if (request_queue.check_compatible(GroupLockMode::S, LockDataType::TABLE)) {
        request_queue.group_lock_mode_ = GroupLockMode::S;
    }

    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) { return true; }

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> latch(latch_);

    // 检查当前事务是否已经获得该表的锁
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    auto& request_queue = lock_table_[lock_data_id];

    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            // 对于IS锁，如果已经持有任何类型的锁，都不需要再申请
            return true;
        }
    }

    // 检查其他请求是否与当前请求冲突
    if (request_queue.check_conflict(GroupLockMode::IS, LockDataType::TABLE)) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    // 没有则创建新的锁请求
    request_queue.push_back(lock_data_id, LockMode::INTENTION_SHARED, txn);

    // 更新请求队列中的锁模式
    if (request_queue.check_compatible(GroupLockMode::IS, LockDataType::TABLE)) {
        request_queue.group_lock_mode_ = GroupLockMode::IS;
    }

    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> latch(latch_);

    // 检查当前事务是否已经获得该表的锁
    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    auto& request_queue = lock_table_[lock_data_id];

    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id() &&
            req.check_compatible(LockMode::INTENTION_EXCLUSIVE, LockDataType::TABLE)) {
            return true;
        }
    }

    // 检查其他请求是否与当前请求冲突
    if (request_queue.check_conflict(GroupLockMode::IX, LockDataType::TABLE)) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    // 没有则创建新的锁请求
    request_queue.push_back(lock_data_id, LockMode::INTENTION_EXCLUSIVE, txn);

    // 更新请求队列中的锁模式
    if (request_queue.check_compatible(GroupLockMode::IX, LockDataType::TABLE)) {
        request_queue.group_lock_mode_ = GroupLockMode::IX;
    }
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) { return true; }