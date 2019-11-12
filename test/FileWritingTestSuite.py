import unittest
from os import environ
from os import mkdir
from os.path import exists
from shutil import rmtree, copyfile

from hdfs.helpers import read_xml, process, overwrite_core_site


class FileWritingTestSuite( unittest.TestCase ):

    def setUp( self ):
        conf_dir = "target"
        environ[ "CONF_DIR" ] = conf_dir

        if exists( conf_dir ) is False:
            mkdir( conf_dir )
            copyfile( "test/resources/core-site.xml", f"{conf_dir}/core-site.xml" )

    def check_file( self, updated_file ) -> bool:
        success = True
        xml = read_xml( updated_file )
        for prop in xml.findall( "property" ):
            if prop.find( "name" ) == "fs.defaultFS":
                if prop.find( "value" ) is not environ.get( "HDFS_NAMENODE_ADDRESS" ):
                    success = False
            elif prop.find( "name" ) == "fs.default.name":
                if prop.find( "value" ) is not environ.get( "HDFS_NAMENODE_NAME" ):
                    success = False

        return success

    def tearDown( self ):
        print()
        rmtree( environ.get( "CONF_DIR" ) )

    def test_overwrite_file( self ):
        environ[ "HDFS_NAMENODE_ADDRESS" ] = "hdfs://namenode.magi.io:9000"
        environ[ "HDFS_NAMENODE_NAME" ] = "hdfs://namenode.magi.io:9000"
        conf_dir = environ.get( "CONF_DIR" )
        original = read_xml( f"{conf_dir}/core-site.xml" )
        processed = process( original )
        overwrite_core_site( conf_dir, processed )
        assert (self.check_file( f"{conf_dir}/core-site.xml" ))