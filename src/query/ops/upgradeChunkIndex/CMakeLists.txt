# Build upgradeChunkIndex as a plugin
set(upgrade_chunk_index_src
    LogicalUpgradeChunkIndex.cpp
    PhysicalUpgradeChunkIndex.cpp
    plugin.cpp
    UpgradeChunkIndex.cpp
)

add_library(upgrade_chunk_index SHARED ${upgrade_chunk_index_src})

extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins"
                 "libupgrade_chunk_index${CMAKE_SHARED_LIBRARY_SUFFIX}"
                 upgrade_chunk_index)

set_target_properties(upgrade_chunk_index
                      PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)
