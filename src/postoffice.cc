/**
 *  Copyright (c) 2015 by Contributors
 */
#include <unistd.h>
#include <thread>
#include <chrono>
#include "ps/internal/postoffice.h"
#include "ps/internal/message.h"
#include "ps/base.h"
#include "postoffice.h"

namespace ps {
Postoffice::Postoffice() { env_ref_ = Environment::_GetSharedRef(); }

void Postoffice::InitEnvironment() {
  const char* val = NULL;
  std::string van_type = GetEnv("DMLC_PS_VAN_TYPE", "zmq");
  van_ = Van::Create(van_type);
  val = CHECK_NOTNULL(Environment::Get()->find("DMLC_ROLE"));
  std::string role(val);
  is_scheduler_ = role == "scheduler";
  is_trainer_ = role=="trainer";
  if (is_scheduler_) {
    val = CHECK_NOTNULL(Environment::Get()->find("START_TRAINER_NUM"));
    init_trainer_num_ = atoi(val);
  }

  verbose_ = GetEnv("PS_VERBOSE", 0);
}

void Postoffice::Start(int customer_id, const char* argv0,
                       const bool do_barrier) {
  start_mu_.lock();
  if (init_stage_ == 0) {
    InitEnvironment();
    // init glog
    if (argv0) {
      dmlc::InitLogging(argv0);
    } else {
      dmlc::InitLogging("ps-lite\0");
    }
    init_stage_++;
  }
  start_mu_.unlock();

  // start van
  van_->Start(customer_id);

  start_mu_.lock();
  if (init_stage_ == 1) {
    // record start time
    start_time_ = time(NULL);
    init_stage_++;
  }
  start_mu_.unlock();
  // do a barrier here
  if (do_barrier)
    Barrier(customer_id, kScheduler+kTrainerGroup);
}

void Postoffice::Finalize(const int customer_id, const bool do_barrier) {
  if (do_barrier)
    Barrier(customer_id, kScheduler+kTrainerGroup);
  if (customer_id == 0) {
    init_trainer_num_ = 0;
    van_->Stop();
    init_stage_ = 0;
    customers_.clear();
    node_ids_.clear();
    barrier_done_.clear();
    server_key_ranges_.clear();
    heartbeats_.clear();
    if (exit_callback_) exit_callback_();
  }
}

void Postoffice::AddCustomer(Customer* customer) {
  std::lock_guard<std::mutex> lk(mu_);
  int app_id = CHECK_NOTNULL(customer)->app_id();
  // check if the customer id has existed
  int customer_id = CHECK_NOTNULL(customer)->customer_id();
  CHECK_EQ(customers_[app_id].count(customer_id), (size_t)0)
      << "customer_id " << customer_id << " already exists\n";
  customers_[app_id].insert(std::make_pair(customer_id, customer));
  std::unique_lock<std::mutex> ulk(barrier_mu_);
  barrier_done_[app_id].insert(std::make_pair(customer_id, false));
}

void Postoffice::RemoveCustomer(Customer* customer) {
  std::lock_guard<std::mutex> lk(mu_);
  int app_id = CHECK_NOTNULL(customer)->app_id();
  int customer_id = CHECK_NOTNULL(customer)->customer_id();
  customers_[app_id].erase(customer_id);
  if (customers_[app_id].empty()) {
    customers_.erase(app_id);
  }
}

Customer* Postoffice::GetCustomer(int app_id, int customer_id,
                                  int timeout) const {
  Customer* obj = nullptr;
  for (int i = 0; i < timeout * 1000 + 1; ++i) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      const auto it = customers_.find(app_id);
      if (it != customers_.end()) {
        std::unordered_map<int, Customer*> customers_in_app = it->second;
        obj = customers_in_app[customer_id];
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return obj;
}

void Postoffice::Barrier(int customer_id, int node_group) {
  if (GetNodeIDs(node_group).size() <= 1) return;
  auto role = van_->my_node().role;
  if (role == Node::SCHEDULER) {
    CHECK(node_group & kScheduler);
  } else if (role == Node::TRAINER) {
    CHECK(node_group & kTrainerGroup);
  } 

  std::unique_lock<std::mutex> ulk(barrier_mu_);
  barrier_done_[0][customer_id] = false;
  Message req;
  req.meta.recver = kScheduler;
  req.meta.request = true;
  req.meta.control.cmd = Control::BARRIER;
  req.meta.app_id = 0;
  req.meta.customer_id = customer_id;
  req.meta.control.barrier_group = node_group;
  req.meta.timestamp = van_->GetTimestamp();
  van_->Send(req);
  barrier_cond_.wait(
      ulk, [this, customer_id] { return barrier_done_[0][customer_id]; });
}

// TODO : modify for elastic server number
// const std::vector<Range>& Postoffice::GetServerKeyRanges() {
//   server_key_ranges_mu_.lock();
//   if (server_key_ranges_.empty()) {
//     for (int i = 0; i < num_servers_; ++i) {
//       server_key_ranges_.push_back(
//           Range(kMaxKey / num_servers_ * i, kMaxKey / num_servers_ * (i + 1)));
//     }
//   }
//   server_key_ranges_mu_.unlock();
//   return server_key_ranges_;
// }

void Postoffice::Manage(const Message& recv) {
  CHECK(!recv.meta.control.empty());
  const auto& ctrl = recv.meta.control;
  if (ctrl.cmd == Control::BARRIER && !recv.meta.request) {
    barrier_mu_.lock();
    auto size = barrier_done_[recv.meta.app_id].size();
    for (size_t customer_id = 0; customer_id < size; customer_id++) {
      barrier_done_[recv.meta.app_id][customer_id] = true;
    }
    barrier_mu_.unlock();
    barrier_cond_.notify_all();
  }
}

std::vector<int> Postoffice::GetDeadNodes(int t) {
  std::vector<int> dead_nodes;
  if (!van_->IsReady() || t == 0) return dead_nodes;

  time_t curr_time = time(NULL);
  const auto& nodes = is_scheduler_ ? GetNodeIDs(kTrainerGroup)
                                    : GetNodeIDs(kScheduler);
  {
    std::lock_guard<std::mutex> lk(heartbeat_mu_);
    for (int r : nodes) {
      auto it = heartbeats_.find(r);
      if ((it == heartbeats_.end() || it->second + t < curr_time) &&
          start_time_ + t < curr_time) {
        dead_nodes.push_back(r);
      }
    }
  }
  return dead_nodes;
}
void Postoffice::ClearNodes() {
  std::lock_guard<std::mutex> lk(node_ids_mu_);
  node_ids_.clear();
  for (int g : {kScheduler, kScheduler + kTrainerGroup}) {
    node_ids_[g].push_back(kScheduler);
  }
  num_trainers_ = 0;
}

void Postoffice::AddNodes(const std::vector<int>& node_ids,
                          const Node::Role role) {
  std::lock_guard<std::mutex> lk(node_ids_mu_);
  if(role == Node::SCHEDULER){
    CHECK_EQ(node_ids.size(), 1);
    CHECK_EQ(node_ids[0], kScheduler);
    for (int g:{kScheduler, kTrainerGroup, kScheduler +  kTrainerGroup }) {
      if (std::find(node_ids_[g].begin(), node_ids_[g].end(), kScheduler) ==
          node_ids_[g].end()) {
        node_ids_[g].push_back(kScheduler);
      }else{
        LOG(WARNING) << "Scheduler already exists";
      }
    }
    return;
  }

  for (int id : node_ids) {
    bool is_new = false;
    for (int g :
         {id, kTrainerGroup, kScheduler+kTrainerGroup}) {
      if (std::find(node_ids_[g].begin(), node_ids_[g].end(), id) ==
          node_ids_[g].end()) {
        node_ids_[g].push_back(id);
        is_new = true;
      }
    }
    if (is_new) {
      ++this->num_trainers_;
    }else{
      LOG(WARNING) << "Node " << id << " already exists";
    }
  }
}

void Postoffice::RemoveNodes(const std::vector<int> node_ids,
                             const Node::Role role) {
  std::lock_guard<std::mutex> lk(node_ids_mu_);

  for (int id : node_ids) {
    bool is_removed = false;
    for (int g : {id, kScheduler + kTrainerGroup, kTrainerGroup}) {
      auto it = std::find(node_ids_[g].begin(), node_ids_[g].end(), id);
      if (it != node_ids_[g].end()) {
        node_ids_[g].erase(it);
        is_removed = true;
      }
    }
    if (is_removed) {
      --this->num_trainers_;
    }
  }
}

int Postoffice::GenNextID() {
  CHECK(is_scheduler_);
  std::lock_guard<std::mutex> lk(node_ids_mu_);
  auto& nodes = this->node_ids_[kTrainerGroup];
  std::sort(nodes.begin(), nodes.end());
  //10 11 13 14 => 12   10 11 12 => 13  get the first missing id number
  int id = nodes[0];
  for (size_t i = 1; i < nodes.size(); ++i) {
    if (nodes[i] != id) {
      break;
    }
    ++id;
  }
  return id;
}

void Postoffice::UpdateOverlay(int node_id, const std::vector<int>& children) {
  CHECK(is_scheduler_);
  this->overlay_graph_[node_id].children = children;
}

constelltion::OverlayTopo Postoffice::GetGlobalOverlay() const {
  return constelltion::OverlayTopo();
}

void Postoffice::UpdateLocalTrans(int parent,
                                  const std::vector<int>& children) {
  CHECK(is_trainer_);
  this->local_trans_topo_.parent = parent this->local_trans_topo_.children =
      children;
}
const int Postoffice::GetMyParent() const {
  CHECK(is_trainer_);
  CHECK_GE(this->local_trans_topo_.parent, 0);
  return this->local_trans_topo_.parent;
}
const std::vector<int>& Postoffice::GetMyChildren() const {
  CHECK(is_trainer_);
  return this->local_trans_topo_.children;
}
}  // namespace ps
