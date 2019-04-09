# Install script for directory: /home/jh/scidbsource

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

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-clientx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE FILE FILES "/home/jh/scidbsource/bin/libscidbclient.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/iquery")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/gen_matrix")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/benchGen")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/loadpipe.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/calculate_chunk_length.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/remove_arrays.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/spaam.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/indexmapper")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/PSF_license.txt")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/__init__.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/scidb_math.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/scidb_progress.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/scidb_schema.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/scidb_afl.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/statistics.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/util.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/counter.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/scidb_psf.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/scidb_control.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/pgpass_updater.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/iquery_client.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/psql_client.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-utilsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/scidblib" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidblib/ssh_runner.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-jdbcx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/jdbc" TYPE FILE FILES "/home/jh/scidbsource/bin/jdbc/scidb4j.jar")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-jdbcx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/jdbc" TYPE FILE FILES "/home/jh/scidbsource/bin/jdbc/example.jar")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-jdbcx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/jdbc" TYPE FILE FILES "/home/jh/scidbsource/bin/jdbc/jdbctest.jar")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-dev-toolsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/tests/unit/unit_tests")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-dev-toolsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidbtestharness")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-dev-toolsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/arg_separator")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-dev-toolsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidbtestprep.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-dev-toolsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/mu_admin.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-dev-toolsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/box_of_points.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-dev-toolsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/etc" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/daemon.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-dev-toolsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/etc" TYPE FILE FILES "/home/jh/scidbsource/bin/mu_config.ini")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-dev-toolsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/etc" TYPE FILE FILES "/home/jh/scidbsource/bin/log4j.properties")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xlibdmallocx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE FILE FILES "/home/jh/scidbsource/bin/libdmalloc.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-testsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/tests" TYPE DIRECTORY FILES "/home/jh/scidbsource/tests/harness/testcases/t")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-testsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/tests" TYPE DIRECTORY FILES "/home/jh/scidbsource/tests/harness/testcases/data")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-testsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/tests" TYPE FILE FILES "/home/jh/scidbsource/tests/harness/testcases/XSLTFile.xsl")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE FILES "/home/jh/scidbsource/bin/plugins/libpoint.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE FILES "/home/jh/scidbsource/bin/plugins/libmatch.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE FILES "/home/jh/scidbsource/bin/plugins/libbestmatch.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE FILES "/home/jh/scidbsource/bin/plugins/librational.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE FILES "/home/jh/scidbsource/bin/plugins/libcomplex.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE FILES "/home/jh/scidbsource/bin/plugins/libra_decl.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE FILES "/home/jh/scidbsource/bin/plugins/libmore_math.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE FILES "/home/jh/scidbsource/bin/plugins/libmisc.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE FILES "/home/jh/scidbsource/bin/plugins/libtile_integration.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE FILES "/home/jh/scidbsource/bin/plugins/libfits.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE FILES "/home/jh/scidbsource/bin/plugins/libmpi.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE RENAME "libdense_linear_algebra-scidb.so" FILES "/home/jh/scidbsource/bin/plugins/libdense_linear_algebra.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  execute_process(COMMAND /home/jh/scidbsource/utils/update_alternatives.sh /opt/scidb/18.1 lib/scidb/plugins dense_linear_algebra .so 18 1 scidb)
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE RENAME "liblinear_algebra-scidb.so" FILES "/home/jh/scidbsource/bin/plugins/liblinear_algebra.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  execute_process(COMMAND /home/jh/scidbsource/utils/update_alternatives.sh /opt/scidb/18.1 lib/scidb/plugins linear_algebra .so 18 1 scidb)
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE FILES "/home/jh/scidbsource/bin/plugins/libupgrade_chunk_index.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE FILE FILES "/home/jh/scidbsource/bin/plugins/libexample_udos.so")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/plugins" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/plugins/mpi_slave_scidb")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/scripts/bellman_ford_example.sh")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-pluginsx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/scripts/pagerank_example.sh")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidbx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidb")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidbx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidb.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidbx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidb_config.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidbx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/disable.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidbx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/pg_seq_reset.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidbx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/system_report.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidbx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/scidb_backup.py")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidbx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE PROGRAM FILES "/home/jh/scidbsource/bin/packaging_only/scidb_cores")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidbx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/share/scidb" TYPE FILE FILES "/home/jh/scidbsource/bin/data/meta.sql")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidbx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/share/scidb" TYPE FILE FILES "/home/jh/scidbsource/bin/log4cxx.properties")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidbx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/scidb/modules" TYPE FILE FILES "/home/jh/scidbsource/bin/packaging_only/prelude.txt")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xscidb-devx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include" TYPE DIRECTORY FILES "/home/jh/scidbsource/include/" REGEX "/\\.svn$" EXCLUDE)
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/home/jh/scidbsource/extern/cmake_install.cmake")
  include("/home/jh/scidbsource/src/cmake_install.cmake")
  include("/home/jh/scidbsource/utils/cmake_install.cmake")
  include("/home/jh/scidbsource/tests/cmake_install.cmake")
  include("/home/jh/scidbsource/examples/cmake_install.cmake")

endif()

if(CMAKE_INSTALL_COMPONENT)
  set(CMAKE_INSTALL_MANIFEST "install_manifest_${CMAKE_INSTALL_COMPONENT}.txt")
else()
  set(CMAKE_INSTALL_MANIFEST "install_manifest.txt")
endif()

string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
file(WRITE "/home/jh/scidbsource/${CMAKE_INSTALL_MANIFEST}"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
