#!/usr/bin/env python3

from os import environ

from common.helpers import read_xml, overwrite_file
from hdfs.helpers import process

if __name__ == '__main__':
    conf_dir = environ.get( "CONF_DIR" ) if environ.get( "CONF_DIR" ) else "/opt/hadoop/etc/hadoop"
    filename = "core-site.xml"
    print( f"using configuration: {conf_dir}/{filename}" )
    xml = read_xml( conf_dir, filename )

    processed_core_site = process( xml )

    if processed_core_site is not None:
        overwrite_file( conf_dir, filename, processed_core_site )
    else:
        print( f"using default {filename} from docker images" )
        print( "to learn more about the HDFS configs please visit the GitHub repo: https://github.com/magi-platform/magi" )