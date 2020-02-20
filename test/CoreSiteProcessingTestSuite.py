# -*- coding: utf-8 -*-
import unittest
from os import environ

from common.helpers import read_xml
from hdfs.helpers import process_core_site
from test.assertions import verify_prop_values


class CoreSiteProcessingTestSuite( unittest.TestCase ):
    core_site_file = "core-site.xml"

    def setUp( self ):
        conf_dir = "test/resources"
        environ[ "CONF_DIR" ] = conf_dir

    def test_replace_namenode_address( self ):
        conf_dir = environ.get( "CONF_DIR" )
        environ[ "HDFS_NAMENODE_ADDRESS" ] = "hdfs://namenode.magi.io:9000"

        xml = read_xml( conf_dir, self.core_site_file )
        output = process_core_site( xml )

        assert (verify_prop_values( output, "fs.defaultFS", environ[ "HDFS_NAMENODE_ADDRESS" ] ))

    def test_replace_namenode_name( self ):
        conf_dir = environ.get( "CONF_DIR" )
        environ[ "HDFS_NAMENODE_NAME" ] = "hdfs://namenode.magi.io:9000"

        xml = read_xml( conf_dir, self.core_site_file )
        output = process_core_site( xml )

        assert (verify_prop_values( output, "fs.default.name", environ[ "HDFS_NAMENODE_NAME" ] ))

    def test_replace_all_supported_properties( self ):
        conf_dir = environ.get( "CONF_DIR" )
        environ[ "HDFS_NAMENODE_ADDRESS" ] = "hdfs://namenode.magi.io:9000"
        environ[ "HDFS_NAMENODE_NAME" ] = "hdfs://namenode.magi.io:9000"

        xml = read_xml( conf_dir, self.core_site_file )
        output = process_core_site( xml )

        assert (verify_prop_values( output, "fs.defaultFS", environ[ "HDFS_NAMENODE_ADDRESS" ] ))
        assert (verify_prop_values( output, "fs.default.name", environ[ "HDFS_NAMENODE_NAME" ] ))


if __name__ == '__main__':
    unittest.main()