add_library(ix ix.cc tools.cpp)
add_dependencies(ix pfm googlelog)
target_link_libraries(ix pfm glog)