# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.10

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /home/jh/.local/share/JetBrains/Toolbox/apps/CLion/ch-0/181.5540.8/bin/cmake/bin/cmake

# The command to remove a file.
RM = /home/jh/.local/share/JetBrains/Toolbox/apps/CLion/ch-0/181.5540.8/bin/cmake/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/jh/scidbsource

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/jh/scidbsource

# Include any dependencies generated for this target.
include extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/depend.make

# Include the progress variables for this target.
include extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/progress.make

# Include the compile flags for this target's objects.
include extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/flags.make

extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o: extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/flags.make
extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o: extern/MurmurHash/MurmurHash3.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/jh/scidbsource/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o"
	cd /home/jh/scidbsource/extern/MurmurHash && /usr/bin/g++-4.9  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o -c /home/jh/scidbsource/extern/MurmurHash/MurmurHash3.cpp

extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.i"
	cd /home/jh/scidbsource/extern/MurmurHash && /usr/bin/g++-4.9 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/jh/scidbsource/extern/MurmurHash/MurmurHash3.cpp > CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.i

extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.s"
	cd /home/jh/scidbsource/extern/MurmurHash && /usr/bin/g++-4.9 $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/jh/scidbsource/extern/MurmurHash/MurmurHash3.cpp -o CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.s

extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o.requires:

.PHONY : extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o.requires

extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o.provides: extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o.requires
	$(MAKE) -f extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/build.make extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o.provides.build
.PHONY : extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o.provides

extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o.provides.build: extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o


# Object files for target MurmurHash_lib
MurmurHash_lib_OBJECTS = \
"CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o"

# External object files for target MurmurHash_lib
MurmurHash_lib_EXTERNAL_OBJECTS =

extern/MurmurHash/libMurmurHash_lib.a: extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o
extern/MurmurHash/libMurmurHash_lib.a: extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/build.make
extern/MurmurHash/libMurmurHash_lib.a: extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/jh/scidbsource/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX static library libMurmurHash_lib.a"
	cd /home/jh/scidbsource/extern/MurmurHash && $(CMAKE_COMMAND) -P CMakeFiles/MurmurHash_lib.dir/cmake_clean_target.cmake
	cd /home/jh/scidbsource/extern/MurmurHash && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/MurmurHash_lib.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/build: extern/MurmurHash/libMurmurHash_lib.a

.PHONY : extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/build

extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/requires: extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/MurmurHash3.cpp.o.requires

.PHONY : extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/requires

extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/clean:
	cd /home/jh/scidbsource/extern/MurmurHash && $(CMAKE_COMMAND) -P CMakeFiles/MurmurHash_lib.dir/cmake_clean.cmake
.PHONY : extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/clean

extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/depend:
	cd /home/jh/scidbsource && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/jh/scidbsource /home/jh/scidbsource/extern/MurmurHash /home/jh/scidbsource /home/jh/scidbsource/extern/MurmurHash /home/jh/scidbsource/extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : extern/MurmurHash/CMakeFiles/MurmurHash_lib.dir/depend
