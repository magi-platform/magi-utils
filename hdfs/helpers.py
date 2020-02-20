import xml.etree.ElementTree as ElementTree
from re import match

from common.helpers import overwrite_prop, get_env


def process_core_site( config_file: ElementTree ) -> ElementTree:
    hdfs_props = get_env( "HDFS_" )
    if len( hdfs_props ) > 0:
        namenode_address = hdfs_props.get( "HDFS_NAMENODE_ADDRESS", None )
        namenode_name = hdfs_props.get( "HDFS_NAMENODE_NAME", None )
        if namenode_address is not None:
            print( "reset namenode addr" )
            if match( "hdfs:\/\/[a-zA-Z0-9.-]+(:\d{0,5})?", namenode_address ):
                overwrite_prop( config_file, "fs.defaultFS", namenode_address )  # careful this mutates core_site
            else:
                print( f"invalid HDFS_NAMENODE_ADDRESS value : {namenode_address}" )
        if namenode_name is not None:
            print( "reset namenode name" )
            overwrite_prop( config_file, "fs.default.name", namenode_name )
        return config_file


def process_hdfs_site( config_file: ElementTree ) -> ElementTree:
    hdfs_props = get_env( "HDFS_" )
    namenode_http_port = hdfs_props.get( "HDFS_NAMENODE_HTTP_PORT", None )
    namenode_https_port = hdfs_props.get( "HDFS_NAMENODE_HTTPS_PORT", None )
    datanode_http_port = hdfs_props.get( "HDFS_DATANODE_HTTP_PORT", None )
    datanode_https_port = hdfs_props.get( "HDFS_DATANODE_HTTPS_PORT", None )
    datanode_address = hdfs_props.get( "HDFS_DATANODE_ADDRESS", None )
    datanode_ipc_port = hdfs_props.get( "HDFS_DATANODE_IPC_PORT", None )
    if namenode_http_port is not None:
        print( "reset namenode http port" )
        overwrite_prop( config_file, "dfs.http.address", f"0.0.0.0:{namenode_http_port}" )
    if namenode_https_port is not None:
        print( "reset namenode https port" )
        overwrite_prop( config_file, "dfs.https.address", f"0.0.0.0:{namenode_https_port}" )
    if datanode_http_port is not None:
        print( "reset datanode http port" )
        overwrite_prop( config_file, "dfs.datanode.http.address", f"0.0.0.0:{datanode_http_port}" )
    if datanode_https_port is not None:
        print( "reset datanode https port" )
        overwrite_prop( config_file, "dfs.datanode.https.address", f"0.0.0.0:{datanode_https_port}" )
    if datanode_address is not None:
        print( "reset datanode address port" )
        overwrite_prop( config_file, "dfs.datanode.address", f"0.0.0.0:{datanode_address}" )
    if datanode_ipc_port is not None:
        print( "reset datanode ipc port" )
        overwrite_prop( config_file, "dfs.datanode.ipc.address", f"0.0.0.0:{datanode_ipc_port}" )

    return config_file