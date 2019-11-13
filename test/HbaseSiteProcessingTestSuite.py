# -*- coding: utf-8 -*-
import unittest
from os import environ

from common.helpers import read_xml
from hbase.helpers import process
from test.assertions import verify_prop_values


class HbaseSiteProcessingTestSuite( unittest.TestCase ):
    hbase_site_file = "hbase-site.xml"

    def setUp( self ):
        conf_dir = "test/resources"
        environ[ "CONF_DIR" ] = conf_dir

    def test_replace_hbase_root_dir( self ):
        conf_dir = environ.get( "CONF_DIR" )
        environ[ "HBASE_ROOT_DIR" ] = "hdfs://namenode.magi.io:9000/hbase"

        xml = read_xml( conf_dir, self.hbase_site_file )
        output = process( xml )

        assert (verify_prop_values( output, "hbase.rootdir", environ[ "HBASE_ROOT_DIR" ] ))

    def test_replace_zookeeper_host( self ):
        conf_dir = environ.get( "CONF_DIR" )
        environ[ "HBASE_ZOOKEEPER_HOST" ] = "zk-test"

        xml = read_xml( conf_dir, self.hbase_site_file )
        output = process( xml )

        assert (verify_prop_values( output, "hbase.zookeeper.quorum", environ[ "HBASE_ZOOKEEPER_HOST" ] ))

    def test_replace_zookeeper_port( self ):
        conf_dir = environ.get( "CONF_DIR" )
        environ[ "HBASE_ZOOKEEPER_PORT" ] = "6308"

        xml = read_xml( conf_dir, self.hbase_site_file )
        output = process( xml )

        assert (verify_prop_values( output, "hbase.zookeeper.property.clientPort", environ[ "HBASE_ZOOKEEPER_PORT" ] ))

    def test_replace_all_supported_properties( self ):
        conf_dir = environ.get( "CONF_DIR" )
        environ[ "HBASE_ROOT_DIR" ] = "hdfs://namenode.magi.io:9000/hbase"
        environ[ "HBASE_ZOOKEEPER_HOST" ] = "zk-test"
        environ[ "HBASE_ZOOKEEPER_PORT" ] = "6308"

        xml = read_xml( conf_dir, self.hbase_site_file )
        output = process( xml )

        assert (verify_prop_values( output, "hbase.rootdir", environ[ "HBASE_ROOT_DIR" ] ))
        assert (verify_prop_values( output, "hbase.zookeeper.quorum", environ[ "HBASE_ZOOKEEPER_HOST" ] ))
        assert (verify_prop_values( output, "hbase.zookeeper.property.clientPort", environ[ "HBASE_ZOOKEEPER_PORT" ] ))


if __name__ == '__main__':
    unittest.main()