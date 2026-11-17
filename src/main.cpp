#include <atomic>
#include <chrono>
#include <coroutine>
#include <deque>
#include <os/os_sync_wait_on_address.h>
#include <print>
#include <queue>
#include <thread>

using Clock = std::chrono::steady_clock;
// Stores all the runnable items that can be ran
class Scheduler {
  struct Timer {
    Clock::time_point when;
    // A coroutine handle is a handle to the frame for the task
    // that needs to be completed
    std::coroutine_handle<> handle;
    // need to be able to sort these Timers so we
    // can choose what should be ran next
    auto operator>(const Timer &other) const -> bool {
      return when > other.when;
    }
  };

  std::deque<std::coroutine_handle<>> ready_to_run;
  std::priority_queue<Timer, std::vector<Timer>, std::greater<>> timers;

  std::atomic_flag stop_flag{false};
  std::atomic<bool> wake_flag{false};

public:
  static auto instance() -> Scheduler & {
    static Scheduler scheduler{};
    return scheduler;
  }

  auto post(std::coroutine_handle<> handle) -> void {
    ready_to_run.push_front(handle);
  }

  auto post_to_be_ran_at(Clock::time_point time, std::coroutine_handle<> handle)
      -> void {
    const auto earliest_time = timers.top().when;
    if (time < earliest_time) {
      timers.emplace(time, handle);
      // probably need to wake up the runner
      if (not wake_flag.load(std::memory_order_relaxed)) {
        // means that the runner explicitly wants to be told to wake up by us,
        // if wake flag is true already that means the runner is running and
        // hasnt gone to sleep yet so there is no point in waking

        // I dont
        wake_flag.store(true, std::memory_order_release);
        // we use any here since only one person (the runner) should be
        // listening to this address
        os_sync_wake_by_address_any(static_cast<void *>(&wake_flag),
                                    sizeof(wake_flag),
                                    OS_SYNC_WAKE_BY_ADDRESS_NONE);
      }
    }
  }
  template <bool order = false,
            auto memory_order = order ? std::memory_order_release
                                      : std::memory_order_relaxed>
  auto signal_stop() {
    stop_flag.test_and_set(memory_order);
  }

  auto run() {

    while (not stop_flag.test()) {
      // find all the timers that are ready to be ran
      const auto now = Clock::now();
      while (not timers.empty() and timers.top().when <= now) {
        ready_to_run.push_back(timers.top().handle);
        timers.pop();
      }

      // resume the most ready to run coroutine:
      if (not ready_to_run.empty()) {
        auto handle = std::move(ready_to_run.front());
        ready_to_run.pop_front();
        handle.resume();
      } else if (not timers.empty()) {
        // wait till the earliest timer is ready to run
        const auto ns_till_earliest_task =
            duration_cast<std::chrono::nanoseconds>(timers.top().when -
                                                    Clock::now())
                .count();

        os_sync_wait_on_address_with_timeout(
            static_cast<void *>(&wake_flag), true, sizeof(wake_flag),
            OS_SYNC_WAIT_ON_ADDRESS_NONE, OS_CLOCK_MACH_ABSOLUTE_TIME,
            static_cast<uint64_t>(ns_till_earliest_task));
      }
    }
  }
};

auto main() -> int { std::println("Hello World"); }
