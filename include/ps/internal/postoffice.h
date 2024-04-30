/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_INTERNAL_POSTOFFICE_H_
#define PS_INTERNAL_POSTOFFICE_H_
#include <mutex>
#include <algorithm>
#include <vector>
#include <unordered_map>
#include <memory>
#include "ps/range.h"
#include "ps/internal/env.h"
#include "ps/internal/customer.h"
#include "ps/internal/van.h"
namespace ps {
/**
 * \brief the center of the system
 */
class Postoffice {
 public:
  void clear_nodes();

  void AddNodes(const std::vector<int>& node_ids, const Node::Role role);

  void RemoveNodes(const std::vector<int> node_ids, const Node::Role role);

  int GenNextID(Node::Role role);

  bool is_scale() const { return van_->my_node().is_scale; }

  /**
   * \brief return the singleton object
   */
  static Postoffice* Get() {
    static Postoffice e;
    return &e;
  }
  /** \brief get the van */
  Van* van() { return van_; }
  /**
   * \brief start the system
   *
   * This function will block until every nodes are started.
   * \param argv0 the program name, used for logging.
   * \param do_barrier whether to block until every nodes are started.
   */
  void Start(int customer_id, const char* argv0, const bool do_barrier);
  /**
   * \brief terminate the system
   *
   * All nodes should call this function before existing.
   * \param do_barrier whether to do block until every node is finalized,
   * default true.
   */
  void Finalize(const int customer_id, const bool do_barrier = true);
  /**
   * \brief add an customer to the system. threadsafe
   */
  void AddCustomer(Customer* customer);
  /**
   * \brief remove a customer by given it's id. threasafe
   */
  void RemoveCustomer(Customer* customer);
  /**
   * \brief get the customer by id, threadsafe
   * \param app_id the application id
   * \param customer_id the customer id
   * \param timeout timeout in sec
   * \return return nullptr if doesn't exist and timeout
   */
  Customer* GetCustomer(int app_id, int customer_id, int timeout = 0) const;
  /**
   * \brief get the id of a node (group), threadsafe
   *
   * if it is a  node group, return the list of node ids in this
   * group. otherwise, return {node_id}
   */
  // to be thread safe
  const std::vector<int>& GetNodeIDs(int node_id) const {
    std::lock_guard<std::mutex> lk(node_ids_mu_);
    const auto it = node_ids_.find(node_id);
    CHECK(it != node_ids_.cend()) << "node " << node_id << " doesn't exist";
    return it->second;
  }
  const int get_init_num_servers() const { return init_num_servers_; }
  const int get_init_num_workers() const { return init_num_workers_; }
  const int get_init_num_trainers() const { return init_num_trainers_; }
  /**
   * \brief return the key ranges of all server nodes
   */
  const std::vector<Range>& GetServerKeyRanges();
  /**
   * \brief the template of a callback
   */
  using Callback = std::function<void()>;
  /**
   * \brief Register a callback to the system which is called after Finalize()
   *
   * The following codes are equal
   * \code {cpp}
   * RegisterExitCallback(cb);
   * Finalize();
   * \endcode
   *
   * \code {cpp}
   * Finalize();
   * cb();
   * \endcode
   * \param cb the callback function
   */
  void RegisterExitCallback(const Callback& cb) { exit_callback_ = cb; }
  /**
   * \brief convert from a worker rank into a node id
   * \param rank the worker rank
   */
  // TODO: delete this function
  static inline int WorkerRankToID(int rank) { return rank * 2 + 9; }
  /**
   * \brief convert from a server rank into a node id
   * \param rank the server rank
   */
  // TODO: delete this function
  static inline int ServerRankToID(int rank) { return rank * 2 + 8; }
  /**
   * \brief convert from a trainer rank into a node id
   * \param rank the trainer rank
   */
  // TODO: delete this function
  static inline int TrainerRankToID(int rank) { return rank * 2 + 10; }
  /**
   * \brief convert from a node id into a server or worker rank
   * \param id the node id
   */
  int IDtoRank(int id, bool is_trainer) {
    // reffer to node_ids_ to get the rank
    std::lock_guard<std::mutex> lk(node_ids_mu_);
    // int node_group = is_worker ? kWorkerGroup : kServerGroup;
    // get the rank of id in the node_group
    std::vector<int>& nodes = node_ids_[kTrainerGroup];
    std::sort(nodes.begin(), nodes.end());
    auto it = std::find(nodes.begin(), nodes.end(), id);
    if (it == nodes.end()) {
      return -1;
    } else {
      return std::distance(nodes.begin(), it);
    }
  }
  /** \brief Returns the number of worker nodes */
  int num_workers() const {
    std::lock_guard<std::mutex> lk(node_ids_mu_);
    return num_workers_;
  }
  /** \brief Returns the number of server nodes */
  int num_servers() const {
    std::lock_guard<std::mutex> lk(node_ids_mu_);
    return num_servers_;
  }

  /** \brief Returns the number of trainer nodes */
  int num_trainers() const {
    std::lock_guard<std::mutex> lk(node_ids_mu_);
    return num_trainers_;
  }

  /** \brief Returns the rank of this node in its group
   *
   * Each worker will have a unique rank within [0, NumWorkers()). So are
   * servers. This function is available only after \ref Start has been called.
   */
  int my_rank() {
    return IDtoRank(van_->my_node().id, van_->my_node().role == Node::WORKER);
  }

  void UpdateScaleNodes(std::vector<int> node_ids, bool is_trainer = false) {
    if (node_ids.empty()) {
      CHECK(is_scale());
      van_->update_scale_status();
      return;
    }
    // check if the node is already in scale nodes
    ScaleMeta& scale_meta = van_->GetScaleMeta();
    CHECK(scale_meta.IstrainerScaling() == is_trainer);
    CHECK(scale_meta.GetNodes().size() == node_ids.size());
    for (int node_id : node_ids) {
      auto it = std::find(scale_meta.GetNodes().begin(),
                          scale_meta.GetNodes().end(), node_id);
      CHECK(it != scale_meta.GetNodes().end())
          << "Node " << node_id << " is not in scale nodes";
    }
    scale_meta.Clear();
    AddNodes(node_ids, is_trainer ? Node::TRAINER : Node::SERVER);

    // for scalling nodes, update
    if (is_scale()) {
      van_->update_scale_status();
    }
  }
  /** \brief Returns true if this node is a worker node */
  int is_worker() const { return is_worker_; }
  /** \brief Returns true if this node is a server node. */
  int is_server() const { return is_server_; }
  /** \brief Returns true if this node is a trainer node. */
  int is_trainer() const { return is_trainer_; }
  /** \brief Returns true if this node is a scheduler node. */
  int is_scheduler() const { return is_scheduler_; }
  /** \brief Returns the verbose level. */
  int verbose() const { return verbose_; }
  /** \brief Return whether this node is a recovery node */
  bool is_recovery() const { return van_->my_node().is_recovery; }
  /**
   * \brief barrier
   * \param node_id the barrier group id
   */
  void Barrier(int customer_id, int node_group);
  /**
   * \brief process a control message, called by van
   * \param the received message
   */
  void Manage(const Message& recv);
  /**
   * \brief update the heartbeat record map
   * \param node_id the \ref Node id
   * \param t the last received heartbeat time
   */
  void UpdateHeartbeat(int node_id, time_t t) {
    std::lock_guard<std::mutex> lk(heartbeat_mu_);
    heartbeats_[node_id] = t;
  }
  /**
   * \brief get node ids that haven't reported heartbeats for over t seconds
   * \param t timeout in sec
   */
  std::vector<int> GetDeadNodes(int t = 60);

 private:
  Postoffice();
  ~Postoffice() { delete van_; }

  void InitEnvironment();
  Van* van_;
  mutable std::mutex mu_;
  // app_id -> (customer_id -> customer pointer)
  std::unordered_map<int, std::unordered_map<int, Customer*>> customers_;
  std::unordered_map<int, std::vector<int>> node_ids_;
  mutable std::mutex node_ids_mu_;  // modify the node_ids_
  std::mutex server_key_ranges_mu_;
  std::vector<Range> server_key_ranges_;
  bool is_worker_, is_server_, is_trainer_, is_scheduler_;
  int num_servers_ = 0;
  int num_workers_ = 0;
  int num_trainers_ = 0;

  // initial number of nodes
  int init_num_servers_, init_num_workers_, init_num_trainers_;

  std::mutex num_servers_mu_;  // modify the number of servers
  std::unordered_map<int, std::unordered_map<int, bool>> barrier_done_;
  int verbose_;
  std::mutex barrier_mu_;
  std::condition_variable barrier_cond_;
  std::mutex heartbeat_mu_;
  std::mutex start_mu_;
  int init_stage_ = 0;
  std::unordered_map<int, time_t> heartbeats_;
  Callback exit_callback_;
  /** \brief Holding a shared_ptr to prevent it from being destructed too early
   */
  std::shared_ptr<Environment> env_ref_;
  time_t start_time_;
  DISALLOW_COPY_AND_ASSIGN(Postoffice);
};

/** \brief verbose log */
#define PS_VLOG(x) LOG_IF(INFO, x <= Postoffice::Get()->verbose())
}  // namespace ps
#endif  // PS_INTERNAL_POSTOFFICE_H_
