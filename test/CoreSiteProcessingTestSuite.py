# -*- coding: utf-8 -*-
import unittest
from os import environ
from hdfs.helpers import process, read_xml
import xml.etree.ElementTree as ElementTree


class CoreSiteProcessingTestSuite( unittest.TestCase ):

    def setUp( self ):
        conf_dir = "test/resources"
        environ[ "CONF_DIR" ] = conf_dir

    def check_prop_value( self, doc: ElementTree, name: str, value: str ) -> bool:
        matches = False
        for prop in doc.findall( "property" ):
            if prop.find( "name" ).text == name and prop.find( "value" ).text == value:
                matches = True
        return matches

    def test_replace_namenode_address( self ):
        conf_dir = environ.get( "CONF_DIR" )
        environ[ "HDFS_NAMENODE_ADDRESS" ] = "hdfs://namenode.magi.io:9000"

        xml = read_xml( f"{conf_dir}/core-site.xml" )
        output = process( xml )
        assert (self.check_prop_value( output, "fs.defaultFS", "hdfs://namenode.magi.io:9000" ))

    def test_replace_namenode_name( self ):
        conf_dir = environ.get( "CONF_DIR" )
        environ[ "HDFS_NAMENODE_NAME" ] = "hdfs://namenode.magi.io:9000"

        xml = read_xml( f"{conf_dir}/core-site.xml" )
        output = process( xml )
        assert (self.check_prop_value( output, "fs.default.name", "hdfs://namenode.magi.io:9000" ))

    def test_replace_all_supported_properties( self ):
        conf_dir = environ.get( "CONF_DIR" )

        environ[ "HDFS_NAMENODE_ADDRESS" ] = "hdfs://namenode.magi.io:9000"
        environ[ "HDFS_NAMENODE_NAME" ] = "hdfs://namenode.magi.io:9000"
        xml = read_xml( f"{conf_dir}/core-site.xml" )

        output = process( xml )

        assert (self.check_prop_value( output, "fs.defaultFS", "hdfs://namenode.magi.io:9000" ))
        assert (self.check_prop_value( output, "fs.default.name", "hdfs://namenode.magi.io:9000" ))


if __name__ == '__main__':
    unittest.main()