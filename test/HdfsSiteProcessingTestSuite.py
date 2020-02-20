# -*- coding: utf-8 -*-
import unittest
from os import environ

from common.helpers import read_xml
from hdfs.helpers import process_hdfs_site
from test.assertions import verify_prop_values


class HdfsSiteProcessingTestSuite( unittest.TestCase ):
    hdfs_site_file = "hdfs-site.xml"

    def setUp( self ):
        conf_dir = "test/resources"
        environ[ "CONF_DIR" ] = conf_dir

    def test_replace_all_supported_properties( self ):
        conf_dir = environ.get( "CONF_DIR" )
        environ[ "HDFS_NAMENODE_HTTP_PORT" ] = "1337"
        environ[ "HDFS_NAMENODE_HTTPS_PORT" ] = "1338"
        environ[ "HDFS_DATANODE_HTTP_PORT" ] = "1339"
        environ[ "HDFS_DATANODE_HTTPS_PORT" ] = "1340"
        environ[ "HDFS_DATANODE_ADDRESS" ] = "1341"
        environ[ "HDFS_DATANODE_IPC_PORT" ] = "1342"

        xml = read_xml( conf_dir, self.hdfs_site_file )
        output = process_hdfs_site( xml )

        assert (verify_prop_values( output, "dfs.http.address", f"0.0.0.0:{environ[ 'HDFS_NAMENODE_HTTP_PORT' ]}" ))
        assert (verify_prop_values( output, "dfs.https.address", f"0.0.0.0:{environ[ 'HDFS_NAMENODE_HTTPS_PORT' ]}" ))
        assert (verify_prop_values( output, "dfs.datanode.http.address", f"0.0.0.0:{environ[ 'HDFS_DATANODE_HTTP_PORT' ]}" ))
        assert (verify_prop_values( output, "dfs.datanode.https.address", f"0.0.0.0:{environ[ 'HDFS_DATANODE_HTTPS_PORT' ]}" ))
        assert (verify_prop_values( output, "dfs.datanode.address", f"0.0.0.0:{environ[ 'HDFS_DATANODE_ADDRESS' ]}" ))
        assert (verify_prop_values( output, "dfs.datanode.ipc.address", f"0.0.0.0:{environ[ 'HDFS_DATANODE_IPC_PORT' ]}" ))


if __name__ == '__main__':
    unittest.main()