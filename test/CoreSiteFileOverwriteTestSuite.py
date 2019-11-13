import unittest
from os import environ
from os import mkdir
from os.path import exists
from shutil import rmtree, copyfile

from common.helpers import read_xml, overwrite_file
from hdfs.helpers import process
from test.assertions import verify_prop_values


class CoreSiteFileOverwriteTestSuite( unittest.TestCase ):
    filename = "core-site.xml"

    def setUp( self ):
        conf_dir = "target"
        environ[ "CONF_DIR" ] = conf_dir

        if exists( conf_dir ) is False:
            mkdir( conf_dir )
            copyfile( "test/resources/core-site.xml", f"{conf_dir}/{self.filename}" )

    def tearDown( self ):
        rmtree( environ.get( "CONF_DIR" ) )

    def test_overwrite_core_site( self ):
        environ[ "HDFS_NAMENODE_ADDRESS" ] = "hdfs://namenode.magi.io:9000"
        environ[ "HDFS_NAMENODE_NAME" ] = "hdfs://namenode.magi.io:9000"
        conf_dir = environ.get( "CONF_DIR" )
        original = read_xml( conf_dir, self.filename )
        processed = process( original )
        overwrite_file( conf_dir, self.filename, processed )

        updated = read_xml( conf_dir, self.filename )
        assert (verify_prop_values( updated, "fs.defaultFS", environ[ "HDFS_NAMENODE_ADDRESS" ] ))
        assert (verify_prop_values( updated, "fs.default.name", environ[ "HDFS_NAMENODE_NAME" ] ))


if __name__ == '__main__':
    unittest.main()