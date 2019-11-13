import unittest
from os import environ
from os import mkdir
from os.path import exists
from shutil import rmtree, copyfile

from common.helpers import read_xml, overwrite_file
from hbase.helpers import process
from test.assertions import verify_prop_values


class HbaseSiteOverrideTesSuite( unittest.TestCase ):
    filename = "hbase-site.xml"

    def setUp( self ):
        conf_dir = "target"
        environ[ "CONF_DIR" ] = conf_dir

        if exists( conf_dir ) is False:
            mkdir( conf_dir )
            copyfile( f"test/resources/{self.filename}", f"{conf_dir}/{self.filename}" )

    def tearDown( self ):
        rmtree( environ.get( "CONF_DIR" ) )

    def test_overwrite_hbase_site( self ):
        conf_dir = environ.get( "CONF_DIR" )
        environ[ "HBASE_ROOT_DIR" ] = "hdfs://namenode.magi.io:9000/hbase"
        environ[ "HBASE_ZOOKEEPER_HOST" ] = "zk-test"
        environ[ "HBASE_ZOOKEEPER_PORT" ] = "6308"
        original = read_xml( conf_dir, self.filename )
        processed = process( original )
        overwrite_file( conf_dir, self.filename, processed )

        updated = read_xml( conf_dir, self.filename )
        assert (verify_prop_values( updated, "hbase.rootdir", environ[ "HBASE_ROOT_DIR" ] ))
        assert (verify_prop_values( updated, "hbase.zookeeper.quorum", environ[ "HBASE_ZOOKEEPER_HOST" ] ))
        assert (verify_prop_values( updated, "hbase.zookeeper.property.clientPort", environ[ "HBASE_ZOOKEEPER_PORT" ] ))


if __name__ == '__main__':
    unittest.main()