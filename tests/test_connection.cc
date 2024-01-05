#include "ps/ps.h"

int main(int argc, char *argv[]) {
  ps::Start(0, "doNotBarrier");
  // do nothing
  while(1){
    std::this_thread::sleep_for(std::chrono::seconds(1000));
  }
  ps::Finalize(0, true);
  return 0;
}
