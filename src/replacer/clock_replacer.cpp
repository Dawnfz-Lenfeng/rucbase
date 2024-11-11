#include "replacer/clock_replacer.h"

#include <algorithm>

ClockReplacer::ClockReplacer(size_t num_pages)
    : circular_{num_pages, ClockReplacer::Status::EMPTY_OR_PINNED}, hand_{0}, capacity_{num_pages} {
    // 成员初始化列表语法
    circular_.reserve(num_pages);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    const std::lock_guard<mutex_t> guard(mutex_);
    // Todo: try to find a victim frame in buffer pool with clock scheme
    // and make the *frame_id = victim_frame_id
    // not found, frame_id=nullptr and return false
    if (!Size()) {
        frame_id = nullptr;
        return false;
    }

    do {
        auto &status = circular_[hand_];
        if (status == ClockReplacer::Status::ACCESSED) {
            status = ClockReplacer::Status::UNTOUCHED;
        } else if (status == ClockReplacer::Status::UNTOUCHED) {
            *frame_id = hand_;
            status = ClockReplacer::Status::EMPTY_OR_PINNED;
            return true;
        }
        hand_ = (hand_ + 1) % capacity_;
    } while (1);
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    const std::lock_guard<mutex_t> guard(mutex_);
    // Todo: you can implement it!
    circular_[frame_id] = ClockReplacer::Status::EMPTY_OR_PINNED;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    const std::lock_guard<mutex_t> guard(mutex_);
    // Todo: you can implement it!
    auto &status = circular_[frame_id];
    if (status == ClockReplacer::Status::EMPTY_OR_PINNED) {
        status = ClockReplacer::Status::ACCESSED;
    }
}

size_t ClockReplacer::Size() {
    // Todo:
    // 返回在[arg0, arg1)范围内满足特定条件(arg2)的元素的数目
    // return all items that in the range[circular_.begin, circular_.end )
    // and be met the condition: status!=EMPTY_OR_PINNED
    // That is the number of frames in the buffer pool that storage page (NOT EMPTY_OR_PINNED)
    return std::count_if(circular_.begin(), circular_.end(), [](const ClockReplacer::Status &status) {
        return status != ClockReplacer::Status::EMPTY_OR_PINNED;
    });
}
