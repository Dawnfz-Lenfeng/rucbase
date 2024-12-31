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
#include <algorithm>

// 锁相容性矩阵
// |     | X | IX | S | IS | SIX |
// |-----|---|----|---|----| --- |
// | X   | 0 | 0  | 0 | 0  |  0  |
// | IX  | 0 | 1  | 0 | 1  |  0  |
// | S   | 0 | 0  | 1 | 1  |  0  |
// | IS  | 0 | 1  | 1 | 1  |  1  |
// | SIX | 0 | 0  | 0 | 1  |  0  |
// |-----|---|----|---|----| --- |

bool LockManager::LockRequest::update_lock_mode(LockMode lock_mode) {
    // 特殊的锁升级情况
    if (lock_mode_ == LockMode::SHARED && lock_mode == LockMode::INTENTION_EXCLUSIVE) {
        lock_mode_ = LockMode::S_IX;  // S + IX -> SIX
        return true;
    }
    if (lock_mode_ == LockMode::INTENTION_EXCLUSIVE && lock_mode == LockMode::SHARED) {
        lock_mode_ = LockMode::S_IX;  // IX + S -> SIX
        return true;
    }

    if (lock_mode_ < lock_mode) {
        lock_mode_ = lock_mode;
        return true;
    }

    return false;
}

bool LockManager::LockRequestQueue::update_group_lock_mode(GroupLockMode group_lock_mode, LockDataType lock_data_type) {
    if (group_lock_mode == GroupLockMode::X && request_queue_.size() > 1) {
        return false;  // X锁与其他锁都不兼容
    }

    GroupLockMode updated_group_lock_mode = group_lock_mode_;
    if (group_lock_mode_ == GroupLockMode::S && group_lock_mode == GroupLockMode::IX) {
        updated_group_lock_mode = GroupLockMode::SIX;  // S + IS -> SIX
    } else if (group_lock_mode_ == GroupLockMode::IX && group_lock_mode == GroupLockMode::S) {
        updated_group_lock_mode = GroupLockMode::SIX;  // IX + S -> SIX
    } else if (group_lock_mode_ < group_lock_mode) {
        updated_group_lock_mode = group_lock_mode;
    }

    if (check_conflict(updated_group_lock_mode, lock_data_type)) {
        return false;
    }

    group_lock_mode_ = updated_group_lock_mode;
    return true;
}

void LockManager::LockRequestQueue::push_back(LockDataId lock_data_id, LockMode lock_mode, Transaction* txn) {
    LockRequest lock_request(txn->get_transaction_id(), lock_mode);
    lock_request.granted_ = true;  // no-wait策略下立即授予锁

    request_queue_.push_back(lock_request);

    txn->get_lock_set()->insert(lock_data_id);
}

bool LockManager::LockRequestQueue::check_conflict(GroupLockMode group_lock_mode, LockDataType lock_data_type) {
    if ((group_lock_mode == GroupLockMode::X && !request_queue_.empty()) || group_lock_mode_ == GroupLockMode::X) {
        return true;  // X锁与其他锁都不兼容
    }

    if (group_lock_mode_ == GroupLockMode::NON_LOCK) {
        return false;  // 没有锁，直接返回false
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
    lock_on_table(txn, tab_fd,
                  lock_mode == LockMode::EXLUCSIVE ? LockMode::INTENTION_EXCLUSIVE : LockMode::INTENTION_SHARED);

    std::unique_lock<std::mutex> latch(latch_);

    // 检查当前事务是否已经获得该记录的锁
    LockDataId lock_data_id(tab_fd, rid, LockDataType::RECORD);
    auto& request_queue = lock_table_[lock_data_id];

    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            if (!req.update_lock_mode(lock_mode)) {
                return;  // 锁模式已经足够强
            }

            GroupLockMode req_group_lock_mode = get_group_lock_mode(req.lock_mode_);
            if (!request_queue.update_group_lock_mode(req_group_lock_mode, LockDataType::RECORD)) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }
            return;
        }
    }

    // 检查冲突
    GroupLockMode group_lock_mode = get_group_lock_mode(lock_mode);
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

void LockManager::lock_on_table(Transaction* txn, int tab_fd, LockMode lock_mode) {
    std::unique_lock<std::mutex> latch(latch_);

    LockDataId lock_data_id(tab_fd, LockDataType::TABLE);
    auto& request_queue = lock_table_[lock_data_id];

    // 检查当前事务的锁
    for (auto& req : request_queue.request_queue_) {
        if (req.txn_id_ == txn->get_transaction_id()) {
            if (!req.update_lock_mode(lock_mode)) {
                return;  // 已经有足够强的锁
            }

            GroupLockMode req_group_lock_mode = get_group_lock_mode(req.lock_mode_);
            if (!request_queue.update_group_lock_mode(req_group_lock_mode, LockDataType::TABLE)) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
            }
            return;
        }
    }

    // 检查冲突
    GroupLockMode group_lock_mode = get_group_lock_mode(lock_mode);
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
void LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) { lock_on_table(txn, tab_fd, LockMode::SHARED); }

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
void LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    lock_on_table(txn, tab_fd, LockMode::EXLUCSIVE);
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
void LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    lock_on_table(txn, tab_fd, LockMode::INTENTION_SHARED);
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
void LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    lock_on_table(txn, tab_fd, LockMode::INTENTION_EXCLUSIVE);
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
}

bool GapLockTable::try_lock_gap(Transaction* txn, int tab_fd, const Rid& start_rid, const Rid& end_rid) {
    std::unique_lock<std::mutex> latch(latch_);

    // 检查是否与已有的间隙锁冲突
    auto& locks = gap_locks_[tab_fd];
    for (const auto& lock : locks) {
        // 检查区间是否重叠
        if (!(end_rid < lock.start_rid_ || start_rid > lock.end_rid_)) {
            return false;  // 有冲突
        }
    }

    // 加入新的间隙锁
    locks.emplace_back(txn->get_transaction_id(), start_rid, end_rid);
    return true;
}

bool GapLockTable::check_gap_conflict(int tab_fd, const Rid& rid) {
    std::unique_lock<std::mutex> latch(latch_);

    auto& locks = gap_locks_[tab_fd];
    for (const auto& lock : locks) {
        // 检查记录是否落在任何已锁定的间隙中
        if (rid > lock.start_rid_ && rid < lock.end_rid_) {
            return true;  // 有冲突
        }
    }
    return false;
}

void GapLockTable::release_gap_locks(txn_id_t txn_id) {
    std::unique_lock<std::mutex> latch(latch_);

    for (auto& pair : gap_locks_) {
        auto& locks = pair.second;
        locks.erase(std::remove_if(locks.begin(), locks.end(),
                                   [txn_id](const GapLock& lock) { return lock.txn_id_ == txn_id; }),
                    locks.end());
    }
}
