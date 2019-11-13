import xml.etree.ElementTree as ElementTree

from os import environ, rename
from re import match
from xml.dom import minidom


def __fix_header( original: str ) -> str:
    old_header = """<?xml version="1.0" ?>"""
    new_header = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n"""

    return original.replace( old_header, new_header )


def read_xml( conf_dir: str, file: str ) -> ElementTree:
    return ElementTree.parse( f"{conf_dir}/{file}" )


def get_env( prefix: str ) -> dict:
    hdfs_props = { }
    for key in environ.keys():
        if isinstance( key, str ) and key.startswith( prefix ):
            hdfs_props[ key ] = environ.get( key )
    return hdfs_props


def overwrite_prop( doc: ElementTree, prop_name: str, value: str ):
    for prop in doc.findall( "property" ):
        if prop.find( "name" ).text == prop_name:
            print( f'replacing {prop_name} : {prop.find( "value" ).text} with {value}' )
            prop.find( "value" ).text = value  ## careful, this modifies the doc


def overwrite_file( conf_dir: str, file: str, content: ElementTree ):
    rename( f"{conf_dir}/{file}", f"{conf_dir}/{file}.original" )
    file = open( f"{conf_dir}/{file}", "w+" )
    xml_str = __fix_header( minidom.parseString( ElementTree.tostring( content.getroot() ) ).toprettyxml( indent = "    ", newl = "" ) )
    file.write( xml_str )
    file.close()