add_library(pfm pfm.cc tools.cpp)
add_dependencies(pfm googlelog)
target_link_libraries(pfm glog)