vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO apache/arrow-adbc
    REF apache-arrow-adbc-${VERSION}
    SHA512 2c325413c4af45642d956263f6a3e56012d7468cc3edf7a4dad325d85aab9469d066af42036983d770f2d8fc366841652455c63cb242ecc4b57b35481795c1cf
    HEAD_REF main
    PATCHES
        toml.patch
#    PATCHES
#        fix_static_build.patch
#        fix_windows_build.patch
#        unvendor.patch
)
file(REMOVE_RECURSE "${SOURCE_PATH}/c/vendor")

# Rename the driver manager's internal SetError() helper to avoid a duplicate
# symbol clash with DuckDB's own bundled ADBC implementation, which also defines
# a global SetError(AdbcError*, const std::string&). Since arrow-adbc 23 the
# driver manager promoted this helper to external linkage (shared with the new
# connection-profile code), which collides at link time. This helper is declared
# only in the internal (non-installed) header, so renaming it is invisible to
# consumers. The regex requires a non-identifier char before the name so it does
# not touch InternalAdbcSetError().
foreach(_dm_file
    c/driver_manager/adbc_driver_manager_internal.h
    c/driver_manager/adbc_driver_manager.cc
    c/driver_manager/adbc_driver_manager_api.cc
    c/driver_manager/adbc_driver_manager_driver_loading.cc
    c/driver_manager/adbc_driver_manager_profiles.cc)
    file(READ "${SOURCE_PATH}/${_dm_file}" _dm_contents)
    string(REGEX REPLACE "([^A-Za-z0-9_])SetError\\(" "\\1AdbcDmSetError(" _dm_contents "${_dm_contents}")
    file(WRITE "${SOURCE_PATH}/${_dm_file}" "${_dm_contents}")
endforeach()

vcpkg_check_features(OUT_FEATURE_OPTIONS FEATURE_OPTIONS
    FEATURES
        "sqlite" "ADBC_DRIVER_SQLITE"
        "postgresql" "ADBC_DRIVER_POSTGRESQL"
        "flightsql" "ADBC_DRIVER_FLIGHTSQL"
)

string(COMPARE EQUAL ${VCPKG_LIBRARY_LINKAGE} "dynamic" ADBC_BUILD_SHARED)
string(COMPARE EQUAL ${VCPKG_LIBRARY_LINKAGE} "static" ADBC_BUILD_STATIC)


vcpkg_cmake_configure(
    SOURCE_PATH ${SOURCE_PATH}/c
    OPTIONS
        ${FEATURE_OPTIONS}
        -DADBC_DRIVER_MANAGER=ON
        -DADBC_BUILD_SHARED=${ADBC_BUILD_SHARED}
        -DADBC_BUILD_STATIC=${ADBC_BUILD_STATIC}
        -DADBC_WITH_VENDORED_NANOARROW=OFF
        -DADBC_WITH_VENDORED_FMT=OFF
        -DADBC_BUILD_WARNING_LEVEL=PRODUCTION
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(
    PACKAGE_NAME AdbcDriverManager
    CONFIG_PATH lib/cmake/AdbcDriverManager
    DO_NOT_DELETE_PARENT_CONFIG_PATH
)
if("postgresql" IN_LIST FEATURES)
    vcpkg_cmake_config_fixup(
        PACKAGE_NAME AdbcDriverPostgreSQL
        CONFIG_PATH lib/cmake/AdbcDriverPostgreSQL
        DO_NOT_DELETE_PARENT_CONFIG_PATH
    )
endif()
if("sqlite" IN_LIST FEATURES)
    vcpkg_cmake_config_fixup(
        PACKAGE_NAME AdbcDriverSQLite
        CONFIG_PATH lib/cmake/AdbcDriverSQLite
        DO_NOT_DELETE_PARENT_CONFIG_PATH
    )
endif()

vcpkg_fixup_pkgconfig()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/lib/cmake")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/lib/cmake")

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE.txt")
