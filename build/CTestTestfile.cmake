# CMake generated Testfile for 
# Source directory: /home/zhuo1/lsmf/leveldb-1.23
# Build directory: /home/zhuo1/lsmf/leveldb-1.23/build
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(leveldb_tests "/home/zhuo1/lsmf/leveldb-1.23/build/leveldb_tests")
set_tests_properties(leveldb_tests PROPERTIES  _BACKTRACE_TRIPLES "/home/zhuo1/lsmf/leveldb-1.23/CMakeLists.txt;361;add_test;/home/zhuo1/lsmf/leveldb-1.23/CMakeLists.txt;0;")
add_test(c_test "/home/zhuo1/lsmf/leveldb-1.23/build/c_test")
set_tests_properties(c_test PROPERTIES  _BACKTRACE_TRIPLES "/home/zhuo1/lsmf/leveldb-1.23/CMakeLists.txt;387;add_test;/home/zhuo1/lsmf/leveldb-1.23/CMakeLists.txt;390;leveldb_test;/home/zhuo1/lsmf/leveldb-1.23/CMakeLists.txt;0;")
add_test(env_posix_test "/home/zhuo1/lsmf/leveldb-1.23/build/env_posix_test")
set_tests_properties(env_posix_test PROPERTIES  _BACKTRACE_TRIPLES "/home/zhuo1/lsmf/leveldb-1.23/CMakeLists.txt;387;add_test;/home/zhuo1/lsmf/leveldb-1.23/CMakeLists.txt;398;leveldb_test;/home/zhuo1/lsmf/leveldb-1.23/CMakeLists.txt;0;")
subdirs("third_party/googletest")
subdirs("third_party/benchmark")
