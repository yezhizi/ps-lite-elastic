/**
 *  Copyright (c) 2015 by Contributors
 */
#include <unistd.h>
#include <thread>
#include <chrono>
#include "ps/internal/postoffice.h"
#include "ps/internal/message.h"
#include "ps/base.h"

namespace ps {
Postoffice::Postoffice() { env_ref_ = Environment::_GetSharedRef(); }

void Postoffice::InitEnvironment() {
  const char* val = NULL;
  std::string van_type = GetEnv("DMLC_PS_VAN_TYPE", "zmq");
  van_ = Van::Create(van_type);
  val = CHECK_NOTNULL(Environment::Get()->find("DMLC_ROLE"));
  std::string role(val);
  is_worker_ = role == "worker";
  is_server_ = role == "server";
  is_scheduler_ = role == "scheduler";

  if (is_scheduler_) {
    // number of workers and servers
    // when the number of worker and server reache the init_num_workers_ and init_num_servers_
    // the training will start, after that the comming node will be added asynchronizely
    val = CHECK_NOTNULL(Environment::Get()->find("DMLC_NUM_WORKER"));
    init_num_workers_ = atoi(val);
    val = CHECK_NOTNULL(Environment::Get()->find("DMLC_NUM_SERVER"));
    init_num_servers_ = atoi(val);
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
    Barrier(customer_id, kWorkerGroup + kServerGroup + kScheduler);
}

void Postoffice::Finalize(const int customer_id, const bool do_barrier) {
  if (do_barrier)
    Barrier(customer_id, kWorkerGroup + kServerGroup + kScheduler);
  if (customer_id == 0) {
    num_workers_ = 0;
    num_servers_ = 0;
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
  } else if (role == Node::WORKER) {
    CHECK(node_group & kWorkerGroup);
  } else if (role == Node::SERVER) {
    CHECK(node_group & kServerGroup);
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
const std::vector<Range>& Postoffice::GetServerKeyRanges() {
  server_key_ranges_mu_.lock();
  if (server_key_ranges_.empty()) {
    for (int i = 0; i < num_servers_; ++i) {
      server_key_ranges_.push_back(
          Range(kMaxKey / num_servers_ * i, kMaxKey / num_servers_ * (i + 1)));
    }
  }
  server_key_ranges_mu_.unlock();
  return server_key_ranges_;
}

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
  const auto& nodes = is_scheduler_ ? GetNodeIDs(kWorkerGroup + kServerGroup)
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
void Postoffice::clear_nodes() {
  std::lock_guard<std::mutex> lk(node_ids_mu_);
  node_ids_.clear();
  for (int g : {kScheduler, kScheduler + kServerGroup + kWorkerGroup,
                kScheduler + kWorkerGroup, kScheduler + kServerGroup}) {
    node_ids_[g].push_back(kScheduler);
  }
  num_servers_ = 0;
  num_workers_ = 0;
}

void Postoffice::AddNodes(const std::vector<int>& node_ids,
                          const Node::Role role) {
  std::lock_guard<std::mutex> lk(node_ids_mu_);
  if(role == Node::SCHEDULER){
    CHECK_EQ(node_ids.size(), 1);
    int node_group = kScheduler;
    CHECK_EQ(node_ids[0], kScheduler);
    for (int g:{kScheduler, kScheduler + kServerGroup + kWorkerGroup,
                kScheduler + kWorkerGroup, kScheduler + kServerGroup}) {
      if (std::find(node_ids_[g].begin(), node_ids_[g].end(), kScheduler) ==
          node_ids_[g].end()) {
        node_ids_[g].push_back(kScheduler);
      }else{
        LOG(WARNING) << "Scheduler already exists";
      }
    }
    return;
  }

  const int node_group = role == Node::SERVER ? kServerGroup : kWorkerGroup;
  const int other_group = role == Node::SERVER ? kWorkerGroup : kServerGroup;
  for (int id : node_ids) {
    bool is_new = false;
    for (int g :
         {id, node_group, node_group + kScheduler, node_group + other_group,
          node_group + kScheduler + other_group}) {
      if (std::find(node_ids_[g].begin(), node_ids_[g].end(), id) ==
          node_ids_[g].end()) {
        node_ids_[g].push_back(id);
        is_new = true;
      }
    }
    if (is_new) {
      if (role == Node::SERVER) {
        ++num_servers_;
      } else {
        ++num_workers_;
      }
    }
  }
}

void Postoffice::RemoveNodes(const std::vector<int> node_ids,
                             const Node::Role role) {
  std::lock_guard<std::mutex> lk(node_ids_mu_);
  const int node_group = role == Node::SERVER ? kServerGroup : kWorkerGroup;
  const int other_group = role == Node::SERVER ? kWorkerGroup : kServerGroup;
  for (int id : node_ids) {
    bool is_removed = false;
    for (int g :
         {id, node_group, node_group + kScheduler, node_group + other_group,
          node_group + kScheduler + other_group}) {
      auto it = std::find(node_ids_[g].begin(), node_ids_[g].end(), id);
      if (it != node_ids_[g].end()) {
        node_ids_[g].erase(it);
        is_removed = true;
      }
    }
    if (is_removed) {
      if (role == Node::SERVER) {
        --num_servers_;
      } else {
        --num_workers_;
      }
    }
  }
}


int Postoffice::GenNextID(Node::Role role) {
  CHECK(role == Node::SERVER || role == Node::WORKER);
  int node_group = role == Node::SERVER ? kServerGroup : kWorkerGroup;
  int id_diff = role == Node::SERVER ? 8 : 9;
  std::lock_guard<std::mutex> lk(node_ids_mu_);
  // woker id = rank * 2 + 9; server id = rank * 2 + 8
  std::vector<int> ids = node_ids_[node_group];
  std::vector<int> scale_nodes = van_->GetScallingNodes();
  ids.insert(ids.end(), scale_nodes.begin(), scale_nodes.end());

  std::sort(ids.begin(), ids.end());
  int id = 0;
  for (int i = 0; i < ids.size(); ++i) {
    if (ids[i] != i * 2 + id_diff) {
      id = i * 2 + id_diff;
      break;
    }
  }
  if (id == 0) {
    id = ids.size() * 2 + id_diff;
  }
  return id;
}

}  // namespace ps
