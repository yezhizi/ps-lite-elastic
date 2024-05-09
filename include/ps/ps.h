/*!
 *  Copyright (c) 2015 by Contributors
 * @file   ps.h
 * \brief  The parameter server interface
 */
#ifndef PS_PS_H_
#define PS_PS_H_
/** \brief basic setups in ps */
#include "ps/base.h"
/** \brief communicating with a pair of (int, string). */
#include "ps/simple_app.h"
/** \brief communcating with a list of key-value paris. */
#include "ps/kv_app.h"
namespace ps {
/** \brief Returns the number of trainer nodes */
inline int NumTrainers() {return Postoffice::Get()->num_trainers();}
/** \brief Returns true if this node is a trainer node */
inline bool IsTrainer() { return Postoffice::Get()->is_trainer(); }
/** \brief Returns true if this node is a scheduler node. */
inline bool IsScheduler() { return Postoffice::Get()->is_scheduler(); }
/** \brief Returns the rank of this node in its group
 *
 * Each worker will have a unique rank within [0, NumWorkers()). So are
 * servers. This function is available only after \ref Start has been called.
 */
inline int MyRank() { return Postoffice::Get()->my_rank(); }
/**
 * \brief start the system
 *
 * This function will block until every nodes are started.
 * \param argv0 the program name, used for logging
 */
inline void Start(int customer_id, const char* argv0 = nullptr) {
  Postoffice::Get()->Start(customer_id, argv0, true);
}
/**
 * \brief start the system
 *
 * This function will NOT block.
 * \param argv0 the program name, used for logging
 */
inline void StartAsync(int customer_id, const char* argv0 = nullptr) {
  Postoffice::Get()->Start(customer_id, argv0, false);
}
/**
 * \brief terminate the system
 *
 * All nodes should call this function before existing. 
 * \param do_barrier whether to block until every node is finalized, default true.
 */
inline void Finalize(int customer_id, const bool do_barrier = true) {
  Postoffice::Get()->Finalize(customer_id, do_barrier);
}
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
inline void RegisterExitCallback(const std::function<void()>& cb) {
  Postoffice::Get()->RegisterExitCallback(cb);
}

}  // namespace ps
#endif  // PS_PS_H_
