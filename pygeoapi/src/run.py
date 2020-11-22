import os
import yaml
import argparse
from itertools import chain

from item.base import Base
from item.spot import Spot 
from item.pleiades import Pleiades

from utility.server import Server
from utility.gsclient import GsClient


def getDataTable( config, items ):

    """
    get data table
    """

    def getPrefix( fields ):

        """
        get prefix
        """

        # get prefix for record insert
        command = 'INSERT INTO {schema}.{table} ( '.format ( schema=config[ 'out' ][ 'schema' ], table=config[ 'out' ][ 'table' ] )
        for idx, (k, v) in enumerate( fields.items() ):

            if v != 'SERIAL':
                command += k

                if idx < len( fields.keys() ) - 1:
                    command += ', '

        command += ' ) VALUES '
        return command 


    # field names / types
    fields = {  'gid' : 'SERIAL',
                'name' : 'TEXT',
                'platform' : 'TEXT',
                'gsd' : 'FLOAT',
                'projection' : 'INT',
                'datetime' : 'TEXT',
                'geom' : 'GEOMETRY',
                'link' : 'TEXT'
     }

    # create server obj
    server = Server( config[ 'server' ] )
    server.createOrReplaceTable( config[ 'out' ][ 'schema' ], config[ 'out' ][ 'table' ], fields )

    # construct command
    command = getPrefix( fields )    
    for idx, item in enumerate ( items ):

        # compile stac values into insertion string
        command += "( '{name}', '{platform}', {gsd}, {projection}, '{datetime}', ST_MakeEnvelope( {coords}, 4326 ), '{link}' )" \
                    .format(    name=item.id, 
                                platform=item.common_metadata.platform, 
                                gsd=item.common_metadata.gsd,
                                projection=item.ext.projection.epsg,
                                datetime=item.datetime.strftime( '%Y-%m-%d %H:%M:%S' ),
                                coords=','.join(map(str, item.bbox)),
                                link=item.assets[ 'image' ].href )
       
        # add comma if not last record
        if idx < len( items ) - 1:
            command += ', '

    return server.executeCommand( command )


def getClient( config ):

    """
    get client
    """

    client = None 

    # create gcs client
    if GsClient.isUri( config[ 'bucket' ] ):

        GsClient.updateCredentials( config[ 'key' ] )
        client = GsClient( config[ 'bucket' ] )

    return client


def getItems( config, path ):

    """
    get items
    """

    items = []

    # select client based on bucket uri
    client = getClient( config )
    if client is None:
        raise ValueError( 'Unable to identify client for bucket: {bucket}'.format( config[ 'bucket' ] ) )

    # get image uri list
    uris = client.getImageUriList( config[ 'prefix' ], config[ 'pattern'] )
    for uri in uris:

        # get class
        _name = Base.getClassName( uri )
        if _name is not None:

            try:                

                # create object
                _class = globals()[ _name ]            
                obj = _class ( path )
                
                # add valid items to list
                items.append( obj.getItem( uri ) )
            
            except Exception as e:
                print ( str( e ) )


    return items


def parseArguments(args=None):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='pygeoapi')
    parser.add_argument( 'config_file', action="store" )

    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments
    args = parseArguments()

    # read configuration
    with open( args.config_file, 'r' ) as f:
        root = yaml.safe_load( f )

    for cat in root[ 'catalog' ]:

        items = list ( chain.from_iterable( getItems( c, os.path.dirname( args.config_file ) ) for c in cat[ 'collection' ][ 'items' ] ) )
        getDataTable( cat[ 'collection' ], items )


    return

# execute main
if __name__ == '__main__':
    main()
