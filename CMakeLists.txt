cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME adbc)

find_package(tomlplusplus REQUIRED)
find_package(AdbcDriverManager CONFIG REQUIRED)
find_package(nanoarrow CONFIG REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/adbc_extension.cpp
    src/adbc_functions.cpp
    src/adbc_scan.cpp
    src/adbc_catalog.cpp
    src/query_farm_telemetry.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link dependencies in both the static library and the loadable extension
target_link_libraries(${EXTENSION_NAME} AdbcDriverManager::adbc_driver_manager_static tomlplusplus::tomlplusplus nanoarrow::nanoarrow)
target_link_libraries(${LOADABLE_EXTENSION_NAME} AdbcDriverManager::adbc_driver_manager_static tomlplusplus::tomlplusplus nanoarrow::nanoarrow)

# On Windows, when linking statically, we need to define ADBC_EXPORT as empty
# to prevent __declspec(dllimport) from being used
if(WIN32)
    target_compile_definitions(${EXTENSION_NAME} PRIVATE ADBC_EXPORT=)
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE ADBC_EXPORT=)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
