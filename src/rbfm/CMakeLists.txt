add_library(rbfm rbfm.cc tools.cpp)
add_dependencies(rbfm googlelog)
target_link_libraries(rbfm glog)