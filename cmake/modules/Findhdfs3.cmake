find_path(HDFS3_INCLUDE_DIR
        NAMES hdfs/hdfs.h)

find_library(HDFS3_LIBRARIES
        NAMES hdfs3)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(hdfs3
        DEFAULT_MSG HDFS3_LIBRARIES HDFS3_INCLUDE_DIR)

mark_as_advanced(
        HDFS3_LIBRARIES
        HDFS3_INCLUDE_DIR)

if(hdfs3_FOUND AND NOT (TARGET hdfs3::hdfs3))
    add_library(hdfs3::hdfs3 UNKNOWN IMPORTED)
    set_target_properties(hdfs3::hdfs3
            PROPERTIES
            IMPORTED_LOCATION ${HDFS3_LIBRARIES}
            INTERFACE_INCLUDE_DIRECTORIES ${HDFS3_INCLUDE_DIR}
            IMPORTED_LINK_INTERFACE_LANGUAGES "CXX")
endif()
