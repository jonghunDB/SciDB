# Install script for directory: /home/jh/scidbsource/examples

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/opt/scidb/18.1")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Debug")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "1")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/home/jh/scidbsource/examples/point/cmake_install.cmake")
  include("/home/jh/scidbsource/examples/rational/cmake_install.cmake")
  include("/home/jh/scidbsource/examples/complex/cmake_install.cmake")
  include("/home/jh/scidbsource/examples/ra_decl/cmake_install.cmake")
  include("/home/jh/scidbsource/examples/more_math/cmake_install.cmake")
  include("/home/jh/scidbsource/examples/misc/cmake_install.cmake")
  include("/home/jh/scidbsource/examples/match/cmake_install.cmake")
  include("/home/jh/scidbsource/examples/bestmatch/cmake_install.cmake")
  include("/home/jh/scidbsource/examples/savebmp/cmake_install.cmake")
  include("/home/jh/scidbsource/examples/operators/cmake_install.cmake")
  include("/home/jh/scidbsource/examples/fits/cmake_install.cmake")
  include("/home/jh/scidbsource/examples/example_udos/cmake_install.cmake")
  include("/home/jh/scidbsource/examples/tile_integration/cmake_install.cmake")

endif()

