#include "include/storage_engine/buffer/frame_manager.h"

FrameManager::FrameManager(const char *tag) : allocator_(tag)
{}

RC FrameManager::init(int pool_num)
{
  int ret = allocator_.init(false, pool_num);
  if (ret == 0) {
    return RC::SUCCESS;
  }
  return RC::NOMEM;
}

RC FrameManager::cleanup()
{
  if (frames_.count() > 0) {
    return RC::INTERNAL;
  }
  frames_.destroy();
  return RC::SUCCESS;
}

Frame *FrameManager::alloc(int file_desc, PageNum page_num)
{
  FrameId frame_id(file_desc, page_num);
  std::lock_guard<std::mutex> lock_guard(lock_);
  Frame *frame = get_internal(frame_id);
  if (frame != nullptr) {
    return frame;
  }

  frame = allocator_.alloc();
  if (frame != nullptr) {
    ASSERT(frame->pin_count() == 0, "got an invalid frame that pin count is not 0. frame=%s",
        to_string(*frame).c_str());
    frame->set_page_num(page_num);
    frame->pin();
    frames_.put(frame_id, frame);
  }
  return frame;
}

Frame *FrameManager::get(int file_desc, PageNum page_num)
{
  FrameId frame_id(file_desc, page_num);
  std::lock_guard<std::mutex> lock_guard(lock_);
  return get_internal(frame_id);
}

RC FrameManager::free(Frame *frame) {
  if (int pin_count = frame->unpin(); pin_count != 0) {
    LOG_WARN("trying to free a frame with pin_count=%d. frame=%s", pin_count, to_string(*frame).c_str());
    return RC::INTERNAL;
  }

  std::lock_guard lock_guard(lock_);
  free_internal(frame);
  return RC::SUCCESS;
}

void FrameManager::free_internal(Frame *frame)
{
  frames_.remove(frame->frame_id());
  allocator_.free(frame);
}

/**
 * TODO [Lab1] 需要同学们实现页帧驱逐
 */
int FrameManager::evict_frames(int count, std::function<RC(Frame *frame)> evict_action)
{
  std::lock_guard lock_guard(lock_);
  std::list<Frame *> evicted_frames;

  frames_.foreach_reverse([&](const FrameId &frame_id, Frame *frame) -> bool {
    // printf("evict_frames got frame=%s\n", to_string(*frame).c_str());
    if (frame->pin_count() == 0) {
      if (RC rc = evict_action(frame); rc == RC::SUCCESS) {
        evicted_frames.push_back(frame);
      } else {
        LOG_WARN("Evict action failed (rc=%s): frame=%s", strrc(rc), to_string(*frame).c_str());
      }
    }
    return evicted_frames.size() < (size_t)count;
  });

  for (auto frame : evicted_frames) {
    free_internal(frame);
  }

  return evicted_frames.size();
}

Frame *FrameManager::get_internal(const FrameId &frame_id)
{
  Frame *frame = nullptr;
  (void)frames_.get(frame_id, frame);
  if (frame != nullptr) {
    frame->pin();
  }
  return frame;
}

/**
 * @brief 查找目标文件的frame
 * FramesCache中选出所有与给定文件描述符(file_desc)相匹配的Frame对象，并将它们添加到列表中
 */
std::list<Frame *> FrameManager::find_list(int file_desc)
{
  std::lock_guard<std::mutex> lock_guard(lock_);

  std::list<Frame *> frames;
  auto fetcher = [&frames, file_desc](const FrameId &frame_id, Frame *const frame) -> bool {
    if (file_desc == frame_id.file_desc()) {
      frame->pin();
      frames.push_back(frame);
    }
    return true;
  };
  frames_.foreach (fetcher);
  return frames;
}
