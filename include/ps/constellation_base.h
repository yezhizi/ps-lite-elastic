#ifndef _CONSTELLATION_BASH_H_
#define _CONSTELLATION_BASH_H_

#include <unordered_map>
#include <vector>

namespace constelltion {

/** @brief Overlay topology*/
using OverlayTopo = std::unordered_map<int, std::vector<int>>;
/** @brief Node transport topology
 * including member parent and children
 * - parent: the parent of this node, `0` - idecates the topo not set, `1` -
 * indicates the root node
 * - children: the children of this node
 */
struct NodeTransTopo {
  int parent = 0;
  std::vector<int> children;
};

}  // namespace constelltion

#endif  // _CONSTELLATION_BASH_H_