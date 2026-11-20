# Use Apache's signed source release, not GitHub's tag archive. The GitHub archive
# embeds the refs pointing at the release commit (export-subst in _static_version.py),
# so its hash changes whenever upstream adds another tag there (go/adbc/v1.13.0 did,
# breaking the build). The release tarball is immutable; its SHA512 matches
# apache-arrow-adbc-${VERSION}.tar.gz.sha512 and the .asc signature on dist.apache.org.
# downloads.apache.org only keeps current releases; archive.apache.org keeps all of them.
vcpkg_download_distfile(ARCHIVE
    URLS
        "https://downloads.apache.org/arrow/apache-arrow-adbc-${VERSION}/apache-arrow-adbc-${VERSION}.tar.gz"
        "https://archive.apache.org/dist/arrow/apache-arrow-adbc-${VERSION}/apache-arrow-adbc-${VERSION}.tar.gz"
    FILENAME "apache-arrow-adbc-${VERSION}.tar.gz"
    SHA512 45f0672c082ac3437dbaf07039e0b36052faaa7c960886ce5745dc398369ca7be038fe45dbe67c45b3c6ba6eebc47541c77c02065a3ff8bb7480adf5baf524d0
)
vcpkg_extract_source_archive(
    SOURCE_PATH
    ARCHIVE "${ARCHIVE}"
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
