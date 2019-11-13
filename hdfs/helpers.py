import xml.etree.ElementTree as ElementTree
from re import match

from common.helpers import overwrite_prop, get_env


def do_property_overrides( core_site: ElementTree, hdfs_props: dict ) -> ElementTree:
    namenode_address = hdfs_props[ "HDFS_NAMENODE_ADDRESS" ] if "HDFS_NAMENODE_ADDRESS" in hdfs_props.keys() else None
    namenode_name = hdfs_props[ "HDFS_NAMENODE_NAME" ] if "HDFS_NAMENODE_NAME" in hdfs_props.keys() else None
    if namenode_address is not None:
        print( "reset namenode addr" )
        if match( "hdfs:\/\/[a-zA-Z0-9.-]+(:\d{0,5})?", namenode_address ):
            overwrite_prop( core_site, "fs.defaultFS", namenode_address )  # careful this mutates core_site
        else:
            print( f"invalid HDFS_NAMENODE_ADDRESS value : {namenode_address}" )
    if namenode_name is not None:
        print( "reset namenode name" )
        overwrite_prop( core_site, "fs.default.name", namenode_name )
    return core_site


def process( core_site: ElementTree ) -> ElementTree:
    hdfs_props = get_env( "HDFS_" )
    if len( hdfs_props ) > 0:
        updated_core_site = do_property_overrides( core_site, hdfs_props )
        return updated_core_site