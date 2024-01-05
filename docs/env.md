# environment variables

The variables must be set for starting

- `DMLC_NUM_WORKER` : the number of workers
- `DMLC_NUM_SERVER` : the number of servers
- `DMLC_ROLE` : the role of the current node, can be `worker`, `server`, or `scheduler`
- `DMLC_PS_ROOT_URI` : the ip or hostname of the scheduler node
- `DMLC_PS_ROOT_PORT` : the port that the scheduler node is listening

additional variables:

- `DMLC_INTERFACE` : the network interface a node should use. in default choose
  automatically
- `DMLC_LOCAL` : runs in local machines, no network is needed
- `DMLC_PS_WATER_MARK`  : limit on the maximum number of outstanding messages
- `DMLC_PS_VAN_TYPE` : the type of the Van for transport, can be `ibverbs` for RDMA, `zmq` for TCP, `p3` for TCP with [priority based parameter propagation](https://anandj.in/wp-content/uploads/sysml.pdf).


# Note: Enable Cmake Debugging in VSCode

1. Install the CMake Tools extension in VSCode

2. Install dependencies

Install Protocol Buffers dependency in the system. For example, in Ubuntu, you can use:

```bash
sudo apt-get install libprotobuf-dev protobuf-compiler
```
3. Clean the previous build

If you have used `make` to build the project, you need use `make clean` to clean the project first.

4. Configure the project

Add the environment variable in the batch file `tests/debug-env-setup/*`. You can use `source` to load the environment variables in the terminal.

Then use "ctrl+shift+p" to open the command palette, and select "CMake: set build target". Then select the target "ALL". Then choose "CMake: set debug target" and select the target you want to debug.

5. Start debugging

Use the extension "CMake Tools" to start debugging. `Build`, `Debug` and `Run` buttons are available in the extension.



