import xml.etree.ElementTree as ElementTree

from os import environ, rename
from re import match
from xml.dom import minidom


def __fix_header( original: str ) -> str:
    old_header = """<?xml version="1.0" ?>"""
    new_header = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n"""

    return original.replace( old_header, new_header )


def read_xml( path: str ) -> ElementTree:
    return ElementTree.parse( path )


def get_hdfs_env() -> dict:
    hdfs_props = { }
    for key in environ.keys():
        if isinstance( key, str ) and key.startswith( "HDFS_" ):
            hdfs_props[ key ] = environ.get( key )
    return hdfs_props


def overwrite_prop( doc: ElementTree, prop_name: str, value: str ):
    for prop in doc.findall( "property" ):
        if prop.find( "name" ).text == prop_name:
            prop.find( "value" ).text = value  ## careful, this modifies the doc


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
    hdfs_props = get_hdfs_env()
    if len( hdfs_props ) > 0:
        updated_core_site = do_property_overrides( core_site, hdfs_props )
        return updated_core_site


def overwrite_core_site( conf_dir: str, content: ElementTree ):
    rename( f"{conf_dir}/core-site.xml", f"{conf_dir}/core-site.orig.xml" )
    file = open( f"{conf_dir}/core-site.xml", "w+" )
    xml_str = __fix_header( minidom.parseString( ElementTree.tostring( content.getroot() ) ).toprettyxml( indent = "    ", newl = "" ) )
    file.write( xml_str )
    file.close()