add_library(rbfm rbfm.cc tools.cpp)
add_dependencies(rbfm pfm googlelog)
target_link_libraries(rbfm pfm glog)