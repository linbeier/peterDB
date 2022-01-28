add_library(rm rm.cc tool.cpp)
add_dependencies(rm rbfm googlelog)
target_link_libraries(rm rbfm glog)