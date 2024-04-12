#include "ps/ps.h"
#include <random>

int generate_random_number(int min, int max) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distrib(min, max);

  return distrib(gen);
}

int main(int argc, char *argv[]) {
  ps::Start(0, "doNotBarrier");
  bool is_scheduler = ps::IsScheduler();
  if(!is_scheduler){
    int sleep_time = generate_random_number(100000, 200000);
    std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
  }else{
    while(true){
      std::this_thread::sleep_for(std::chrono::seconds(1000));
    }
  }
  
  ps::Finalize(0, false);
  return 0;
}