from intake.catalog.local import Catalog, DataSource, LocalCatalogEntry
from intake.source.base import Schema
from intake.container import container_map

class MyDriver(DataSource):
    # Short identifier
    name = 'mydriver'

    # Definition: the basic type of data that the class returns, such as "dataframe"
    # If containter = catalog then the type of the object that you read from on
    # the client side is a RemoteCatalogEntry.
    # If container = python then you get a RemoteSequenceSource on the Client
    # side.
    # This determine the types of the class that you get on the client side.
    # container is looked up in intake.container.container_map to determine the
    # class you get on the client side.
    container = 'catalog'

    # This defaults to false and you are supposed to set it to true to read the data in
    # chunks. It doesn't seem to make a difference though.
    partition_access = True

    def __init__(self, shape, color, **kwargs):
        self._shape = shape
        self._color = color
        super().__init__(**kwargs)

    # Returns a partition of the data.
    def _get_partition(self, partition):
        print("GET_PARTITION", partition['index'])
        partitions = [self._shape, self._color]
        return partitions[partition['index']]

    # Overridding read does nothing.
    # The client calls read on the RemoteCatalogEntry, or the
    # RemoteSequenceSource, not on the DataSource.
    # print(remote_catalog['outer']()['circle']()['green'].read()) doesn't call
    # this method.
    def read(self):
        print("READ")
        return [self._get_partition({'index': i}) for i in range(self.npartitions)]

    # I think read partition only takes an integer, but is able to call
    # get_partition with something more complex than an integer.
    # This is what you need to do if you want a more complex partition.
    def read_partition(self, i):
        print("READ PARTITION MY DRIVER")
        return self._get_partition({'index': i})

    # Returns the schema of the container.
    # Somehow the result is passed to RemoteSequenceSource.
    def _get_schema(self):
        return Schema(
            datashape=(2,),
            npartitions=2,  # This sets self.npartitions of the RemoteSequenceSource
            extra_metadata={})


class InnerCatalog(Catalog):
    # Short identifier
    name = 'inner'

    def __init__(self, shape, **kwargs):
        self._shape = shape
        super().__init__(**kwargs)

    # Load the entries of the catalog.
    def _load(self):
        print(f'loaded inner catalog for {self._shape}')
        for color in ('red', 'green', 'blue'):
            self._entries[color] = LocalCatalogEntry(
                name=color,
                driver='remote_test.cat.MyDriver',
                description='',
                catalog=self,
                direct_access='forbid',  # This needs to be set to forbid.
                # Args are the kwargs for the MyDriver entries.
                args={'shape': self._shape, 'color': color})

    # This method loads the data from the source,
    # and returns the partition defined by the partition argument.
    def read_partition(self, partition):
        print(partition['index'])


class OuterCatalog(Catalog):
    # Short identifier
    name = 'outer'

    # Load the entries of the catalog.
    def _load(self):
        for shape in ('circle', 'square', 'triangle'):
            self._entries[shape] = LocalCatalogEntry(
                name=shape,
                driver='remote_test.cat.InnerCatalog',
                description='',
                catalog=self,
                direct_access='forbid', # This needs to be set to forbid.
                # args are the kwargs for the InnerCatalog
                args={'shape': shape})

    # This method loads the data from the source,
    # and returns the partition defined by the partition argument.
    def read_partition(self, partition):
        print(partition['index'])
