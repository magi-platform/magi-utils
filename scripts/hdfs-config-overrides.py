#!/usr/bin/env python3

from os import environ

from common.helpers import read_xml, overwrite_file
from hdfs.helpers import process_core_site, process_hdfs_site

if __name__ == '__main__':
    conf_dir = environ.get( "CONF_DIR" ) if environ.get( "CONF_DIR" ) else "/opt/hadoop/etc/hadoop"
    core_site_file = "core-site.xml"
    hdfs_site_file = "hdfs-site.xml"
    print( f"using configuration: {conf_dir}/{core_site_file}" )
    print( f"using configuration: {conf_dir}/{hdfs_site_file}" )
    core_site = read_xml( conf_dir, core_site_file )
    hdfs_site = read_xml( conf_dir, hdfs_site_file )

    processed_core_site = process_core_site( core_site )
    processed_hdfs_site = process_hdfs_site( hdfs_site )

    if processed_core_site is not None:
        overwrite_file( conf_dir, core_site_file, processed_core_site )
    if processed_hdfs_site is not None:
        overwrite_file( conf_dir, hdfs_site_file, processed_hdfs_site )
    else:
        print( f"using default {core_site_file} from docker images" )
        print( "to learn more about the HDFS configs please visit the GitHub repo: https://github.com/magi-platform/magi" )