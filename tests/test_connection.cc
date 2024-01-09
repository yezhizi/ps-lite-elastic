#include "ps/ps.h"

int main(int argc, char *argv[]) {
  ps::Start(0, "doNotBarrier");
  //do nothing
  ps::Finalize(0, false);
  return 0;
}
