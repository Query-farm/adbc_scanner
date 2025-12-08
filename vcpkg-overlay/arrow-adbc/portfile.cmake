vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO apache/arrow-adbc
    REF apache-arrow-adbc-${VERSION}
    SHA512 74d9dedd15bce71bfbc5bce00ad1aa91be84623010e2a01e6846343a7acc93e36fb263a08cc8437a9467bf63a2c7aca4b14d413325d5afb96b590408d918b27e
    HEAD_REF main
    PATCHES
        toml.patch
#    PATCHES
#        fix_static_build.patch
#        fix_windows_build.patch
#        unvendor.patch
)
file(REMOVE_RECURSE "${SOURCE_PATH}/c/vendor")

vcpkg_check_features(OUT_FEATURE_OPTIONS FEATURE_OPTIONS
    FEATURES
        "sqlite" "ADBC_DRIVER_SQLITE"
        "postgresql" "ADBC_DRIVER_POSTGRESQL"
        "flightsql" "ADBC_DRIVER_FLIGHTSQL"
        "snowflake" "ADBC_DRIVER_SNOWFLAKE"
        "bigquery" "ADBC_DRIVER_BIGQUERY"
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
