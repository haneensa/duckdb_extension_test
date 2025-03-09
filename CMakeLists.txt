cmake_minimum_required(VERSION 3.5)

# Set extension name
set(TARGET_NAME lineage)

# Find OpenSSL
find_package(OpenSSL REQUIRED)

# Include directories
include_directories(src/include)

# List of source files
set(EXTENSION_SOURCES 
    src/lineage_extension.cpp
    src/lineage_reader.cpp
    src/lineage_meta.cpp
    src/lineage_query.cpp
    src/physical_lineage_operator.cpp
    src/logical_lineage_operator.cpp
)

# Build static and loadable extensions
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} "" ${EXTENSION_SOURCES})

# Link OpenSSL
target_link_libraries(${TARGET_NAME}_extension OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${TARGET_NAME}_loadable_extension OpenSSL::SSL OpenSSL::Crypto)

# Install targets
install(
  TARGETS ${TARGET_NAME}_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
