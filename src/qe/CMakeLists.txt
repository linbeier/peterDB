add_library(qe qe.cc tools.cpp)
add_dependencies(qe ix rm googlelog)
target_link_libraries(qe ix rm glog)