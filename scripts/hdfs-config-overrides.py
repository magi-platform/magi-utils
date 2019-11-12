#!/usr/bin/env python3

from os import environ

from hdfs.helpers import process, overwrite_core_site, read_xml

if __name__ == '__main__':
    CONF_DIR = environ.get( "CONF_DIR" ) if environ.get( "CONF_DIR" ) else "/opt/hadoop/conf/hadoop"
    print( f"using configuration: {CONF_DIR}/core-site.xml" )
    xml = read_xml( f"{CONF_DIR}/core-site.xml" )

    processed_core_site = process( xml )

    if processed_core_site is not None:
        overwrite_core_site( CONF_DIR, processed_core_site )
    else:
        print( "using default core-site.xml from docker images" )
        print( "to learn more about the HDFS configs please visit the GitHub repo: https://github.com/magi-platform/magi" )