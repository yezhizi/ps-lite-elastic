/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_INTERNAL_VAN_H_
#define PS_INTERNAL_VAN_H_
#include <atomic>
#include <ctime>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "ps/base.h"
#include "ps/internal/message.h"
namespace ps {
class ScaleMeta{
  public:
  ScaleMeta() : worker_asyc_scale_(false), server_asyc_scale_(false), is_worker_(false) {}
  ~ScaleMeta() = default;
  std::vector<int>& GetNodes() { return nodes_; }
  bool AddNode(int node, bool is_worker) {
    if(!nodes_.empty() && is_worker_ != is_worker) {
      return false;
    }
    nodes_.push_back(node);
    is_worker_ = is_worker;
    return true;
  }
  void Clear() {
    nodes_.clear();
    is_worker_ = false;
  }
  bool IsWorkerScaling() { return is_worker_; }
  bool& IsAsycScale(bool is_worker) {
    if(is_worker) {
      return worker_asyc_scale_;
    } else {
      return server_asyc_scale_;
    }
  }
  bool IsTwoRoleAllAsycScale() {
    return worker_asyc_scale_ && server_asyc_scale_;
  }
  private:
    bool worker_asyc_scale_ ;
    bool server_asyc_scale_ ;

    std::vector<int> nodes_;
    bool is_worker_;

};
class Resender;
class PBMeta;
/**
 * \brief Van sends messages to remote nodes
 *
 * If environment variable PS_RESEND is set to be 1, then van will resend a
 * message if it no ACK messsage is received within PS_RESEND_TIMEOUT
 * millisecond
 */
class Van {
 public:
  /**
   * \brief create Van
   * \param type zmq, socket, ...
   */
  static Van *Create(const std::string &type);

  /** \brief constructer, do nothing. use \ref Start for real start */
  Van() {}

  /**\brief deconstructer, do nothing. use \ref Stop for real stop */
  virtual ~Van() {}

  /**
   * \brief start van
   *
   * must call it before calling Send
   *
   * it initializes all connections to other nodes.  start the receiving
   * thread, which keeps receiving messages. if it is a system
   * control message, give it to postoffice::manager, otherwise, give it to the
   * corresponding app.
   */
  virtual void Start(int customer_id);

  /**
   * \brief send a message, It is thread-safe
   * \return the number of bytes sent. -1 if failed
   */
  int Send(const Message &msg);

  /**
   * \brief return my node
   */
  inline const Node &my_node() const {
    CHECK(ready_) << "call Start() first";
    return my_node_;
  }

  inline void update_scale_status() {
    CHECK(ready_);
    // the scheduler promise there is no command sent to the van when the scale is processing
    my_node_.is_scale = false;
  }

  void SendDelMsg();
  /**
   * \brief stop van
   * stop receiving threads
   */
  virtual void Stop();

  /**
   * \brief get next available timestamp. thread safe
   */
  inline int GetTimestamp() { return timestamp_++; }

  /**
   * \brief whether it is ready for sending. thread safe
   */
  inline bool IsReady() { return ready_; }

  inline bool IsTopoUpdated() {
      return is_scale_processing_;
  }

  ScaleMeta & GetScaleMeta() { return scale_meta_; }


 protected:
  /**
   * \brief connect to a node
   */
  virtual void Connect(const Node &node) = 0;

  virtual void Disconnect(const Node &node){};

  /**
   * \brief bind to my node
   * do multiple retries on binding the port. since it's possible that
   * different nodes on the same machine picked the same port
   * \return return the port binded, -1 if failed.
   */
  virtual int Bind(const Node &node, int max_retry) = 0;

  /**
   * \brief block until received a message
   * \return the number of bytes received. -1 if failed or timeout
   */
  virtual int RecvMsg(Message *msg) = 0;

  /**
   * \brief send a mesage
   * \return the number of bytes sent
   */
  virtual int SendMsg(const Message &msg) = 0;

  /**
   * \brief pack meta into a string
   */
  void PackMeta(const Meta &meta, char **meta_buf, int *buf_size);

  /**
   * \brief pack meta into protobuf
   */
  void PackMetaPB(const Meta &meta, PBMeta *pb);

  /**
   * \brief unpack meta from a string
   */
  void UnpackMeta(const char *meta_buf, int buf_size, Meta *meta);

  Node scheduler_;
  Node my_node_;
  bool is_scheduler_;
  std::mutex start_mu_;

 private:
  /** thread function for receving */
  void Receiving();

  /** thread function for heartbeat */
  void Heartbeat();

  // node's address string (i.e. ip:port) -> node id
  // this map is updated when ip:port is received for the first time
  std::unordered_map<std::string, int> connected_nodes_;
  // maps the id of node which is added later to the id of node
  // which is with the same ip:port and added first
  std::unordered_map<int, int> shared_node_mapping_;

  /** whether it is ready for sending */
  std::atomic<bool> ready_{false};
  std::atomic<size_t> send_bytes_{0};
  size_t recv_bytes_ = 0;
  int num_trainers_=0; 
  /** the thread for receiving messages */
  std::unique_ptr<std::thread> receiver_thread_;
  /** the thread for sending heartbeat */
  std::unique_ptr<std::thread> heartbeat_thread_;
  std::vector<int> barrier_count_;
  /** msg resender */
  Resender *resender_ = nullptr;
  int drop_rate_ = 0;
  std::atomic<int> timestamp_{0};
  int init_stage = 0;

  Meta expect_nodes_;

  std::atomic<bool> is_scale_processing_{false};

  Meta nodes_list_;

  // scalling zone

  ScaleMeta scale_meta_;

  bool is_first_barrier_done_ = false;

  Meta& GetExpectNodes();

  int SendSingnaltoController(kControllerSignal signal, const std::string &body);
  /**
   * \brief processing logic of AddNode message for scheduler
   */
  void ProcessAddNodeCommandAtScheduler(Message *msg, Meta *nodes,
                                        Meta *recovery_nodes);
  void ProcessDelNodeCommandAtScheduler(Message *msg, Meta *nodes);
  
  /**
   * \brief processing logic of Terminate message
   */
  void ProcessTerminateCommand();

  void ProcessDelNodeCommand(Message *msg, Meta *nodes);

  /**
   * \brief processing logic of AddNode message (run on each node)
   */
  void ProcessAddNodeCommand(Message *msg, Meta *nodes, Meta *recovery_nodes);

  /**
   * \brief processing logic of Barrier message (run on each node)
   */
  void ProcessBarrierCommand(Message *msg);

  /**
   * \brief processing logic of AddNode message (run on each node)
   */
  void ProcessHearbeat(Message *msg);

  /**
   * \brief processing logic of Data message
   */
  void ProcessDataMsg(Message *msg);

  /**
   * \brief called by ProcessAddNodeCommand, in scheduler it assigns an id to
   * the newly added node, and collect this nodes into the Meta * nodes;
   * in other nodes, it updates the node id with what is received from scheduler
   */
  void UpdateLocalID(Message *msg, std::unordered_set<int> *deadnodes_set,
                     Meta *nodes, Meta *recovery_nodes);

  const char *heartbeat_timeout_val =
      Environment::Get()->find("PS_HEARTBEAT_TIMEOUT");
  int heartbeat_timeout_ =
      heartbeat_timeout_val ? atoi(heartbeat_timeout_val) : 0;

  DISALLOW_COPY_AND_ASSIGN(Van);
};
}  // namespace ps
#endif  // PS_INTERNAL_VAN_H_
