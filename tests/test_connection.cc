#include "ps/ps.h"

int main(int argc, char *argv[]) {
  ps::StartAsync(0);
  //do nothing
  ps::Finalize(0, false);
  return 0;
}
