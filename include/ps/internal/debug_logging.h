#ifndef PS_INTERNAL_DEBUG_LOGGING_H_
#define PS_INTERNAL_DEBUG_LOGGING_H_

#include "dmlc/logging.h"
#include <unordered_map>
#include <sstream>
#include <string>

namespace ps {
class LogMapHelper {
 public:
  // Constructor: accept a delimiter
  LogMapHelper(const std::string& delimiter) : delimiter_(delimiter) {}

  // Overload operator<< for std::unordered_map
  LogMapHelper& operator<<(const std::unordered_map<std::string, int>& map) {
    ss_ << " { ";
    for (const auto& pair : map) {
      ss_ << pair.first << delimiter_ << pair.second << ", ";
    }
    // Remove the trailing comma and space
    std::string output = ss_.str();
    if (!output.empty()) {
      output = output.substr(
          0, output.length() - 2);  // if there is at least one pair
    }
    ss_.str("");
    ss_ << output;
    ss_ << " }";
    return *this;
  }

  // Overload operator<< for std::string
  LogMapHelper& operator<<(const std::string& str) {
    ss_ << str;
    return *this;
  }

  // Destructor: output the log message
  ~LogMapHelper() { LOG(INFO) << ss_.str(); }

 private:
  std::ostringstream ss_;
  std::string delimiter_;
};

#define LOG_MAP(delimiter) LogMapHelper(delimiter)

}  // namespace ps

#endif  // PS_INTERNAL_DEBUG_LOGGING_H_
