/**
 *  Copyright (c) 2015 by Contributors
 */

#include <chrono>
#include <thread>

#include "ps/base.h"
#include "ps/internal/customer.h"
#include "ps/internal/postoffice.h"
#include "ps/internal/van.h"
#include "ps/sarray.h"

#include "ps/internal/debug_logging.h"

#include "./meta.pb.h"
#include "./network_utils.h"
#include "./ibverbs_van.h"
#include "./resender.h"
#include "./zmq_van.h"
#include "./p3_van.h"

namespace ps {

// interval in second between to heartbeast signals. 0 means no heartbeat.
// don't send heartbeast in default. because if the scheduler received a
// heartbeart signal from a node before connected to that node, then it could be
// problem.
static const int kDefaultHeartbeatInterval = 0;

Van* Van::Create(const std::string& type) {
  if (type == "zmq") {
    return new ZMQVan();
  } else if (type == "p3") {
    return new P3Van();
#ifdef DMLC_USE_IBVERBS
  } else if (type == "ibverbs") {
    return new IBVerbsVan();
#endif
  } else {
    LOG(FATAL) << "Unsupported van type: " << type;
    return nullptr;
  }
}

void Van::ProcessTerminateCommand() {
  PS_VLOG(1) << my_node().ShortDebugString() << " is stopped";
  ready_ = false;
}

void Van::ProcessAddNodeCommandAtScheduler(Message* msg, Meta* nodes,
                                           Meta* recovery_nodes) {
  recovery_nodes->control.cmd = Control::ADD_NODE;
  time_t t = time(NULL);

  auto& ctrl = msg->meta.control;
  std::vector<Node> comingNodes(ctrl.node.begin(), ctrl.node.begin() + 1);
  std::vector<Node> expect_nodes(ctrl.node.begin() + 1, ctrl.node.end());

  if (recovery_nodes->control.node.empty()) {
    CHECK_EQ(comingNodes.size(), 1);
    for (auto& node : comingNodes) {
      auto& role = node.role;
      if (role == Node::SCHEDULER) continue;

      std::string node_host_ip =
          node.hostname + ":" + std::to_string(node.port);
      if (connected_nodes_.find(node_host_ip) == connected_nodes_.end()) {
        // TODO: need modify GetNextID
        auto id = Postoffice::Get()->GenNextID();
        node.id = id;
        PS_VLOG(1) << "assign id=" << id << " to node " << node.DebugString();
        Connect(node);
        Postoffice::Get()->UpdateHeartbeat(node.id, t);
        connected_nodes_[node_host_ip] = id;
      } else {
        LOG(WARNING) << "node " << node.DebugString()
                     << " already connected before";
      }
      // update the recorded nodes
      if (role == Node::TRAINER) ++num_trainers_;

      Postoffice::Get()->AddNodes({node.id});
      if(std::find_if(nodes->control.node.begin(), nodes->control.node.end(),
                      [&node](const Node& n) { return n.id == node.id; }) ==
         nodes->control.node.end()) {
        nodes->control.node.push_back(node);
      }

      // check if all the expected nodes are registered
      std::vector<int> targets;
      if (Environment::Get()->find("DEBUG_OVERLAY")) {
        // send add node msg to all nodes
        for (auto& n : nodes->control.node) {
          if (n.id != node.id) {
            targets.push_back(n.id);
          }
        }
      } else {
        for (auto& expect_node : expect_nodes) {
          auto expect_node_host_ip =
              expect_node.hostname + ":" + std::to_string(expect_node.port);
          if (connected_nodes_.find(expect_node_host_ip) ==
              connected_nodes_.end()) {
            LOG(WARNING)
                << "The node " << node.DebugString()
                << " expected to be connected to the unregistered node "
                << expect_node.DebugString() << " is not connected";
            continue;
          }
          targets.push_back(connected_nodes_[expect_node_host_ip]);
        }
      }

      //send add node msg to the expected nodes
      Message back;
      back.meta.control.cmd = Control::ADD_NODE;
      for(auto target:targets){
        back.meta.control.node.push_back(node);
        back.meta.recver = target;
        back.meta.timestamp = timestamp_++;
        Send(back);
      }
      //send back to the new node
      back.meta.control.node.clear();
      for(auto& n: nodes->control.node){
        if(std::find(targets.begin(), targets.end(), n.id) != targets.end()){
          back.meta.control.node.push_back(n);
        }
      }
      back.meta.recver = node.id;
      back.meta.timestamp = timestamp_++;
      Send(back);

      //TODO:update overlay

      if(Postoffice::Get()->num_trainers()>=Postoffice::Get()->init_num_trainers()){
        //TODO: 需要判断是否是同步加入阶段
        SendSingnaltoController(kControllerSignal::kAddNodeSignal, "");
      }
    }
    ready_ = true;
  } else {
    auto dead_nodes = Postoffice::Get()->GetDeadNodes(heartbeat_timeout_);
    std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
    // send back the recovery node
    CHECK_EQ(recovery_nodes->control.node.size(), 1);
    Connect(recovery_nodes->control.node[0]);
    Postoffice::Get()->UpdateHeartbeat(recovery_nodes->control.node[0].id, t);
    Message back;
    for (int r : Postoffice::Get()->GetNodeIDs(kTrainerGroup)) {
      if (r != recovery_nodes->control.node[0].id &&
          dead_set.find(r) != dead_set.end()) {
        // do not try to send anything to dead node
        continue;
      }
      //TODO: need send only overlay topology, not all nodes
      // only send recovery_node to nodes already exist
      // but send all nodes to the recovery_node
      back.meta =
          (r == recovery_nodes->control.node[0].id) ? *nodes : *recovery_nodes;
      back.meta.recver = r;
      back.meta.timestamp = timestamp_++;
      Send(back);
    }
  }
}

void Van::ProcessDelNodeCommandAtScheduler(Message* msg, Meta* nodes) {
  // need modify the logic
  // auto& ctrl = msg->meta.control;
  // CHECK_EQ(ctrl.node.size(), 1);
  // auto& outcoming_node = ctrl.node[0];
  // CHECK_NE(outcoming_node.id, Meta::kEmpty) << "node id is empty";
  // auto it = std::find_if(nodes->control.node.begin(), nodes->control.node.end(),
  //                        [&outcoming_node](const Node& node) {
  //                          return node.id == outcoming_node.id;
  //                        });
  // CHECK(it != nodes->control.node.end())
  //     << "node " << outcoming_node.DebugString() << " not found";
  // CHECK_EQ(it->hostname, outcoming_node.hostname);
  // CHECK_EQ(it->port, outcoming_node.port);

  // // send del node to all nodes
  // Message back;
  // back.meta = msg->meta;
  // for (int r : Postoffice::Get()->GetNodeIDs(kTrainerGroup)) {
  //   back.meta.recver = r;
  //   back.meta.timestamp = timestamp_++;
  //   Send(back);
  // }
  // // remove the node from nodes
  // nodes->control.node.erase(it);
}

void Van::UpdateLocalID(Message* msg, std::unordered_set<int>* deadnodes_set,
                        Meta* nodes, Meta* recovery_nodes) {
  auto& ctrl = msg->meta.control;
  // assign an id
  if (msg->meta.sender == Meta::kEmpty) {
    CHECK(is_scheduler_);  // only scheduler can assign id
    CHECK_EQ(ctrl.node.size(), 1);
    // Add the scheduler node to the nodes list
    if (nodes->control.node.empty()) nodes->control.node.push_back(my_node_);

    for (auto& node : nodes->control.node) {
      if (deadnodes_set->find(node.id) != deadnodes_set->end() &&
          node.role == ctrl.node[0].role) {
        auto& recovery_node = ctrl.node[0];
        // assign previous node id
        recovery_node.id = node.id;
        recovery_node.is_recovery = true;
        PS_VLOG(1) << "replace dead node " << node.DebugString() << " by node "
                   << recovery_node.DebugString();
        node = recovery_node;
        recovery_nodes->control.node.push_back(recovery_node);
        break;
      }
    }
  }

  // update my id
  for (size_t i = 0; i < ctrl.node.size(); ++i) {
    const auto& node = ctrl.node[i];
    if (my_node_.hostname == node.hostname && my_node_.port == node.port) {
      // always update the my_node_ info
      my_node_ = node;

      // TODO: may no need to setenv DMLC_RANK
      // setenv may is not thread safe
      // keep the IDtoRank and RanktoID mapping
//       std::string rank = std::to_string(
//           Postoffice::Get()->IDtoRank(node.id, my_node_.role == Node::WORKER));
// #ifdef _MSC_VER
//       _putenv_s("DMLC_RANK", rank.c_str());
// #else
//       setenv("DMLC_RANK", rank.c_str(), true);
// #endif
    }
  }
}

void Van::ProcessHearbeat(Message* msg) {
  auto& ctrl = msg->meta.control;
  time_t t = time(NULL);
  for (auto& node : ctrl.node) {
    Postoffice::Get()->UpdateHeartbeat(node.id, t);
    if (is_scheduler_) {
      Message heartbeat_ack;
      heartbeat_ack.meta.recver = node.id;
      heartbeat_ack.meta.control.cmd = Control::HEARTBEAT;
      heartbeat_ack.meta.control.node.push_back(my_node_);
      heartbeat_ack.meta.timestamp = timestamp_++;
      // send back heartbeat
      Send(heartbeat_ack);
    }
  }
}

void Van::ProcessBarrierCommand(Message* msg) {
  auto& ctrl = msg->meta.control;
  if (!msg->meta.request) {
    Postoffice::Get()->Manage(*msg);
    return;
  }
  if (barrier_count_.empty()) {
    barrier_count_.resize(8, 0);
  }
  int group = ctrl.barrier_group;
  int count_total;
  count_total = static_cast<int>(Postoffice::Get()->GetNodeIDs(group).size());
  ++barrier_count_[group];
  PS_VLOG(1) << "Barrier count for " << group << " : " << barrier_count_[group];
  if (barrier_count_[group] == count_total) {
    barrier_count_[group] = 0;
    Message res;
    res.meta.request = false;
    res.meta.app_id = msg->meta.app_id;
    res.meta.customer_id = msg->meta.customer_id;
    res.meta.control.cmd = Control::BARRIER;
    for (int r : Postoffice::Get()->GetNodeIDs(group)) {
      int recver_id = r;
      if (shared_node_mapping_.find(r) == shared_node_mapping_.end()) {
        res.meta.recver = recver_id;
        res.meta.timestamp = timestamp_++;
        Send(res);
      }
    }
    if (!is_first_barrier_done_) {
      PS_VLOG(1) << "The first barrier is done, training started!";
      is_first_barrier_done_ = true;
    }
  }
}

void Van::ProcessDataMsg(Message* msg) {
  // data msg
  CHECK_NE(msg->meta.sender, Meta::kEmpty);
  CHECK_NE(msg->meta.recver, Meta::kEmpty);
  CHECK_NE(msg->meta.app_id, Meta::kEmpty);
  int app_id = msg->meta.app_id;
  int customer_id =
      Postoffice::Get()->is_trainer() ? msg->meta.customer_id : app_id;
  auto* obj = Postoffice::Get()->GetCustomer(app_id, customer_id, 5);
  CHECK(obj) << "timeout (5 sec) to wait App " << app_id << " customer "
             << customer_id << " ready at " << my_node_.role;
  obj->Accept(*msg);
}

void Van::ProcessDelNodeCommand(Message* msg, Meta* nodes) {
  // need modify the logic
  // is_scale_processing_ = false;
  // auto& ctrl = msg->meta.control;
  // auto& outcoming_nodes = ctrl.node;
  // if (is_scheduler_) {
  //   ProcessDelNodeCommandAtScheduler(msg, nodes);
  // } else {
  //   int myid = my_node_.id;
  //   auto it =
  //       std::find_if(outcoming_nodes.begin(), outcoming_nodes.end(),
  //                    [myid](const Node& node) { return node.id == myid; });
  //   if (it != outcoming_nodes.end()) {
  //     // I am going to be deleted
  //     LOG(INFO) << "Received DEL_NODE command, going to be deleted";
  //     ready_ = true;
  //     return;
  //   }
  // }
  // // update the topo, del the node
  // std::vector<int> server_ids;
  // std::vector<int> worker_ids;
  // for (auto& node : outcoming_nodes) {
  //   auto addr_str = node.hostname + ":" + std::to_string(node.port);
  //   connected_nodes_.erase(addr_str);
  //   Disconnect(node);
  //   LOG(INFO) << "Disconnected from " << node.DebugString();
  //   if (node.role == Node::SERVER) {
  //     server_ids.push_back(node.id);
  //     --num_servers_;
  //   } else {
  //     worker_ids.push_back(node.id);
  //     --num_workers_;
  //   }
  // }
  // Postoffice::Get()->RemoveNodes(server_ids, Node::SERVER);
  // Postoffice::Get()->RemoveNodes(worker_ids, Node::WORKER);

  // LOG_MAP(" id=") << "connected nodes" << connected_nodes_;
  // if (is_scheduler_) {
  //   LOG(INFO) << "the scheduler is connected to " << num_workers_
  //             << " workers and " << num_servers_ << " servers";
  // }
}

void Van::ProcessAddNodeCommand(Message* msg, Meta* nodes,
                                Meta* recovery_nodes) {
  auto dead_nodes = Postoffice::Get()->GetDeadNodes(heartbeat_timeout_);
  std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
  auto& ctrl = msg->meta.control;

  UpdateLocalID(msg, &dead_set, nodes, recovery_nodes);

  if (is_scheduler_) {
    ProcessAddNodeCommandAtScheduler(msg, nodes, recovery_nodes);
  } else {
    // Incremental Update
    std::vector<int> targets;
    for (const auto& node : ctrl.node) {
      std::string addr_str = node.hostname + ":" + std::to_string(node.port);
      if (connected_nodes_.find(addr_str) == connected_nodes_.end()) {
        if (!node.is_recovery) {
          // TODO:check if the nodes are expected
          auto& expect_nodes = this->expect_nodes_.control.node;
          if (std::find_if(expect_nodes.begin(), expect_nodes.end(),
                           [&node](const Node& n) {
                             return n.hostname == node.hostname &&
                                    n.port == node.port;
                           }) == expect_nodes.end()) {
            LOG(WARNING) << "The node " << node.DebugString()
                         << " is not expected to be connected";
            continue;
          }
          Connect(node);
          ++num_trainers_;
          targets.push_back(node.id);
        };
      }
      connected_nodes_[addr_str] = node.id;
    }
    LOG_MAP(" id=") << "connected nodes" << connected_nodes_;

    Postoffice::Get()->AddNodes(targets);
    ready_ = true;
    is_scale_processing_ = true;
  }
}

void Van::Start(int customer_id) {
  // get scheduler info
  start_mu_.lock();

  if (init_stage == 0) {
    scheduler_.hostname = std::string(
        CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_URI")));
    scheduler_.port =
        atoi(CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_PORT")));
    scheduler_.role = Node::SCHEDULER;
    scheduler_.id = kScheduler;
    scheduler_.is_scale = false;
    is_scheduler_ = Postoffice::Get()->is_scheduler();

    // get my node info
    if (is_scheduler_) {
      my_node_ = scheduler_;
    } else {
      auto role = Node::TRAINER;
      const char* nhost = Environment::Get()->find("DMLC_NODE_HOST");
      std::string ip;
      if (nhost) ip = std::string(nhost);
      if (ip.empty()) {
        const char* itf = Environment::Get()->find("DMLC_INTERFACE");
        std::string interface;
        if (itf) interface = std::string(itf);
        if (interface.size()) {
          GetIP(interface, &ip);
        } else {
          GetAvailableInterfaceAndIP(&interface, &ip);
        }
        CHECK(!interface.empty()) << "failed to get the interface";
      }
      int port = GetAvailablePort();
      const char* pstr = Environment::Get()->find("PORT");
      if (pstr) port = atoi(pstr);
      CHECK(!ip.empty()) << "failed to get ip";
      CHECK(port) << "failed to get a port";
      my_node_.hostname = ip;
      my_node_.role = role;
      my_node_.port = port;
      // cannot determine my id now, the scheduler will assign it later
      // set it explicitly to make re-register within a same process possible
      my_node_.id = Node::kEmpty;
      my_node_.customer_id = customer_id;

      // get expected nodes
      this->GetExpectNodes();
    }
    
    // bind.
    my_node_.port = Bind(my_node_, is_scheduler_ ? 0 : 40);
    PS_VLOG(1) << "Bind to " << my_node_.DebugString();
    CHECK_NE(my_node_.port, -1) << "bind failed";

    // connect to the scheduler
    Connect(scheduler_);
    ps::Postoffice::Get()->AddNodes({scheduler_.id}, Node::SCHEDULER);

    // for debug use
    if (Environment::Get()->find("PS_DROP_MSG")) {
      drop_rate_ = atoi(Environment::Get()->find("PS_DROP_MSG"));
    }
    // start receiver
    receiver_thread_ =
        std::unique_ptr<std::thread>(new std::thread(&Van::Receiving, this));
    init_stage++;
  }
  start_mu_.unlock();

  if (!is_scheduler_) {
    // let the scheduler know myself
    Message msg;
    Node customer_specific_node = my_node_;
    customer_specific_node.customer_id = customer_id;
    msg.meta.recver = kScheduler;
    msg.meta.control.cmd = Control::ADD_NODE;
    // the node[0] is my node and remainings are the nodes that expected to be
    // connected
    msg.meta.control.node.push_back(customer_specific_node);
    // get the nodes that expected to be connected
    auto& expect_nodes = this->GetExpectNodes().control.node;
    msg.meta.control.node.insert(msg.meta.control.node.end(),
                                 expect_nodes.begin(), expect_nodes.end());
    msg.meta.timestamp = timestamp_++;
    Send(msg);
  }

  // wait until ready
  while (!ready_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  start_mu_.lock();
  if (init_stage == 1) {
    // resender
    if (Environment::Get()->find("PS_RESEND") &&
        atoi(Environment::Get()->find("PS_RESEND")) != 0) {
      int timeout = 1000;
      if (Environment::Get()->find("PS_RESEND_TIMEOUT")) {
        timeout = atoi(Environment::Get()->find("PS_RESEND_TIMEOUT"));
      }
      resender_ = new Resender(timeout, 10, this);
    }

    if (!is_scheduler_) {
      // start heartbeat thread
      heartbeat_thread_ =
          std::unique_ptr<std::thread>(new std::thread(&Van::Heartbeat, this));
    }
    init_stage++;
  }
  start_mu_.unlock();
}

void Van::SendDelMsg() {
  CHECK(ready_.load());
  ready_ = false;
  Message gexit;
  gexit.meta.control.cmd = Control::DEL_NODE;
  gexit.meta.control.node.push_back(my_node_);
  gexit.meta.recver = kScheduler;
  gexit.meta.customer_id = 0;
  int ret = SendMsg(gexit);
  CHECK_NE(ret, -1);
  while (!ready_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void Van::Stop() {

  // stop threads
  Message exit;
  exit.meta.control.cmd = Control::TERMINATE;
  exit.meta.recver = my_node_.id;
  // only customer 0 would call this method
  exit.meta.customer_id = 0;
  int ret = SendMsg(exit);
  CHECK_NE(ret, -1);
  receiver_thread_->join();
  init_stage = 0;
  if (!is_scheduler_) heartbeat_thread_->join();
  if (resender_) delete resender_;
  ready_ = false;
  connected_nodes_.clear();
  shared_node_mapping_.clear();
  send_bytes_ = 0;
  timestamp_ = 0;
  my_node_.id = Meta::kEmpty;
  barrier_count_.clear();
}

int Van::Send(const Message& msg) {
  int send_bytes = SendMsg(msg);
  CHECK_NE(send_bytes, -1);
  send_bytes_ += send_bytes;
  if (resender_) resender_->AddOutgoing(msg);
  if (Postoffice::Get()->verbose() >= 2) {
    if (!msg.meta.simple_app) {
      PS_VLOG(3) << msg.DebugString();
    } else {
      PS_VLOG(2) << msg.DebugString();
    }
  }
  return send_bytes;
}

void Van::Receiving() {
  Meta& nodes = this->nodes_list_;
  Meta recovery_nodes;  // store recovery nodes
  recovery_nodes.control.cmd = Control::ADD_NODE;
  while (true) {
    Message msg;
    int recv_bytes = RecvMsg(&msg);
    // For debug, drop received message
    if (ready_.load() && drop_rate_ > 0) {
      unsigned seed = time(NULL) + my_node_.id;
      if (rand_r(&seed) % 100 < drop_rate_) {
        LOG(WARNING) << "Drop message " << msg.DebugString();
        continue;
      }
    }

    CHECK_NE(recv_bytes, -1);
    recv_bytes_ += recv_bytes;
    if (Postoffice::Get()->verbose() >= 2) {
      if (!msg.meta.simple_app) {
        PS_VLOG(3) << msg.DebugString();
      } else {
        if (msg.meta.head == 13)
          PS_VLOG(3) << msg.DebugString();
        else
          PS_VLOG(2) << msg.DebugString();
      }
    }
    // duplicated message
    if (resender_ && resender_->AddIncomming(msg)) continue;
    if (!msg.meta.control.empty()) {
      // control msg
      auto& ctrl = msg.meta.control;
      if (ctrl.cmd == Control::TERMINATE) {
        ProcessTerminateCommand();
        break;
      } else if (ctrl.cmd == Control::ADD_NODE) {
        ProcessAddNodeCommand(&msg, &nodes, &recovery_nodes);
      } else if (ctrl.cmd == Control::DEL_NODE) {
        ProcessDelNodeCommand(&msg, &nodes);
      } else if (ctrl.cmd == Control::BARRIER) {
        ProcessBarrierCommand(&msg);
      } else if (ctrl.cmd == Control::HEARTBEAT) {
        ProcessHearbeat(&msg);
      } else {
        LOG(WARNING) << "Drop unknown typed message " << msg.DebugString();
      }
    } else {
      ProcessDataMsg(&msg);
    }
  }
}

void Van::PackMetaPB(const Meta& meta, PBMeta* pb) {
  pb->set_head(meta.head);
  if (meta.app_id != Meta::kEmpty) pb->set_app_id(meta.app_id);
  if (meta.timestamp != Meta::kEmpty) pb->set_timestamp(meta.timestamp);
  if (meta.body.size()) pb->set_body(meta.body);
  pb->set_push(meta.push);
  pb->set_request(meta.request);
  pb->set_simple_app(meta.simple_app);
  pb->set_priority(meta.priority);
  pb->set_customer_id(meta.customer_id);
  for (auto d : meta.data_type) pb->add_data_type(d);
  if (!meta.control.empty()) {
    auto ctrl = pb->mutable_control();
    ctrl->set_cmd(meta.control.cmd);
    if (meta.control.cmd == Control::BARRIER) {
      ctrl->set_barrier_group(meta.control.barrier_group);
    } else if (meta.control.cmd == Control::ACK) {
      ctrl->set_msg_sig(meta.control.msg_sig);
    }
    for (const auto& n : meta.control.node) {
      auto p = ctrl->add_node();
      p->set_id(n.id);
      p->set_role(n.role);
      p->set_port(n.port);
      p->set_hostname(n.hostname);
      p->set_is_recovery(n.is_recovery);
      p->set_customer_id(n.customer_id);
    }
  }
  pb->set_data_size(meta.data_size);
}

void Van::PackMeta(const Meta& meta, char** meta_buf, int* buf_size) {
  // convert into protobuf
  PBMeta pb;
  pb.set_head(meta.head);
  if (meta.app_id != Meta::kEmpty) pb.set_app_id(meta.app_id);
  if (meta.timestamp != Meta::kEmpty) pb.set_timestamp(meta.timestamp);
  if (meta.body.size()) pb.set_body(meta.body);
  pb.set_push(meta.push);
  pb.set_pull(meta.pull);
  pb.set_request(meta.request);
  pb.set_simple_app(meta.simple_app);
  pb.set_priority(meta.priority);
  pb.set_customer_id(meta.customer_id);
  for (auto d : meta.data_type) pb.add_data_type(d);
  if (!meta.control.empty()) {
    auto ctrl = pb.mutable_control();
    ctrl->set_cmd(meta.control.cmd);
    if (meta.control.cmd == Control::BARRIER) {
      ctrl->set_barrier_group(meta.control.barrier_group);
    } else if (meta.control.cmd == Control::ACK) {
      ctrl->set_msg_sig(meta.control.msg_sig);
    }
    for (const auto& n : meta.control.node) {
      auto p = ctrl->add_node();
      p->set_id(n.id);
      p->set_role(n.role);
      p->set_port(n.port);
      p->set_hostname(n.hostname);
      p->set_is_recovery(n.is_recovery);
      p->set_customer_id(n.customer_id);
      p->set_is_scale(n.is_scale);
    }
  }

  // to string
  *buf_size = pb.ByteSize();
  *meta_buf = new char[*buf_size + 1];
  CHECK(pb.SerializeToArray(*meta_buf, *buf_size))
      << "failed to serialize protobuf";
}

void Van::UnpackMeta(const char* meta_buf, int buf_size, Meta* meta) {
  // to protobuf
  PBMeta pb;
  CHECK(pb.ParseFromArray(meta_buf, buf_size))
      << "failed to parse string into protobuf";

  // to meta
  meta->head = pb.head();
  meta->app_id = pb.has_app_id() ? pb.app_id() : Meta::kEmpty;
  meta->timestamp = pb.has_timestamp() ? pb.timestamp() : Meta::kEmpty;
  meta->request = pb.request();
  meta->push = pb.push();
  meta->pull = pb.pull();
  meta->simple_app = pb.simple_app();
  meta->priority = pb.priority();
  meta->body = pb.body();
  meta->customer_id = pb.customer_id();
  meta->data_type.resize(pb.data_type_size());
  for (int i = 0; i < pb.data_type_size(); ++i) {
    meta->data_type[i] = static_cast<DataType>(pb.data_type(i));
  }
  if (pb.has_control()) {
    const auto& ctrl = pb.control();
    meta->control.cmd = static_cast<Control::Command>(ctrl.cmd());
    meta->control.barrier_group = ctrl.barrier_group();
    meta->control.msg_sig = ctrl.msg_sig();
    for (int i = 0; i < ctrl.node_size(); ++i) {
      const auto& p = ctrl.node(i);
      Node n;
      n.role = static_cast<Node::Role>(p.role());
      n.port = p.port();
      n.hostname = p.hostname();
      n.id = p.has_id() ? p.id() : Node::kEmpty;
      n.is_recovery = p.is_recovery();
      n.customer_id = p.customer_id();
      n.is_scale = p.is_scale();
      meta->control.node.push_back(n);
    }
  } else {
    meta->control.cmd = Control::EMPTY;
  }
}

void Van::Heartbeat() {
  const char* val = Environment::Get()->find("PS_HEARTBEAT_INTERVAL");
  const int interval = val ? atoi(val) : kDefaultHeartbeatInterval;
  while (interval > 0 && ready_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(interval));
    Message msg;
    msg.meta.recver = kScheduler;
    msg.meta.control.cmd = Control::HEARTBEAT;
    msg.meta.control.node.push_back(my_node_);
    msg.meta.timestamp = timestamp_++;
    Send(msg);
  }
}
int Van::SendSingnaltoController(kControllerSignal signal,
                                 const std::string& body) {
  CHECK_EQ(my_node_.role, Node::SCHEDULER);
  Message msg;
  msg.meta.app_id = 0;
  msg.meta.head = static_cast<int>(signal);
  if (!body.empty()) msg.meta.body = body;
  msg.meta.timestamp = timestamp_++;
  msg.meta.request = true;
  msg.meta.customer_id = 0;
  msg.meta.recver = kScheduler;
  msg.meta.simple_app = true;
  int ret = Send(msg);
  CHECK_NE(ret, -1);
  PS_VLOG(1) << "Send signal to controller " << static_cast<int>(signal);
  return ret;
}
Meta& Van::GetExpectNodes() {
  if (this->expect_nodes_.control.node.size() != 0) {
    return this->expect_nodes_;
  }
  // DMLC_PS_EXPECTED_NODES=192.169.1.1:6000;192.169.1.2:7000
  std::string node_str = GetEnv("DMLC_PS_EXPECTED_NODES", "");
  if (node_str.length() > 0) {
    // split the nodes by ;
    std::vector<std::string> node_list;
    std::string::size_type start = 0;
    std::string::size_type end = node_str.find(";");
    while (end != std::string::npos) {
      node_list.push_back(node_str.substr(start, end - start));
      start = end + 1;
      end = node_str.find(";", start);
    }
    node_list.push_back(node_str.substr(start));
    for (auto& node : node_list) {
      std::string::size_type pos = node.find(":");
      CHECK(pos != std::string::npos)
          << "invalid format of node " << node << ", should be ip:port";
      std::string ip = node.substr(0, pos);
      int port = atoi(node.substr(pos + 1).c_str());
      Node n;
      n.hostname = ip;
      n.port = port;
      n.role = Node::TRAINER;
      expect_nodes_.control.node.push_back(n);
    }
  }
  return this->expect_nodes_;
}
}  // namespace ps
