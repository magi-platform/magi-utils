import unittest
from os import environ
from os import mkdir
from os.path import exists
from shutil import rmtree, copyfile

from common.helpers import read_xml, overwrite_file
from test.assertions import verify_prop_values
from hdfs.helpers import process_hdfs_site


class CoreSiteFileOverwriteTestSuite( unittest.TestCase ):
    filename = "hdfs-site.xml"

    def setUp( self ):
        conf_dir = "target"
        environ[ "CONF_DIR" ] = conf_dir

        if exists( conf_dir ) is False:
            mkdir( conf_dir )
            copyfile( "test/resources/hdfs-site.xml", f"{conf_dir}/{self.filename}" )

    def tearDown( self ):
        rmtree( environ.get( "CONF_DIR" ) )

    def test_overwrite_core_site( self ):
        environ[ "HDFS_NAMENODE_HTTP_PORT" ] = "1337"
        environ[ "HDFS_NAMENODE_HTTPS_PORT" ] = "1338"
        environ[ "HDFS_DATANODE_HTTP_PORT" ] = "1339"
        environ[ "HDFS_DATANODE_HTTPS_PORT" ] = "1340"
        environ[ "HDFS_DATANODE_ADDRESS" ] = "1341"
        environ[ "HDFS_DATANODE_IPC_PORT" ] = "1342"

        conf_dir = environ.get( "CONF_DIR" )

        original = read_xml( conf_dir, self.filename )
        processed = process_hdfs_site( original )
        overwrite_file( conf_dir, self.filename, processed )

        updated = read_xml( conf_dir, self.filename )

        assert (verify_prop_values( updated, "dfs.http.address", f"0.0.0.0:{environ[ 'HDFS_NAMENODE_HTTP_PORT' ]}" ))
        assert (verify_prop_values( updated, "dfs.https.address", f"0.0.0.0:{environ[ 'HDFS_NAMENODE_HTTPS_PORT' ]}" ))
        assert (verify_prop_values( updated, "dfs.datanode.http.address", f"0.0.0.0:{environ[ 'HDFS_DATANODE_HTTP_PORT' ]}" ))
        assert (verify_prop_values( updated, "dfs.datanode.https.address", f"0.0.0.0:{environ[ 'HDFS_DATANODE_HTTPS_PORT' ]}" ))
        assert (verify_prop_values( updated, "dfs.datanode.address", f"0.0.0.0:{environ[ 'HDFS_DATANODE_ADDRESS' ]}" ))
        assert (verify_prop_values( updated, "dfs.datanode.ipc.address", f"0.0.0.0:{environ[ 'HDFS_DATANODE_IPC_PORT' ]}" ))

        if __name__ == '__main__':
            unittest.main()