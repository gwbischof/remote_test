from intake.catalog.local import Catalog, DataSource, LocalCatalogEntry
from intake.source.base import Schema
from intake.container import container_map

class MyDriver(DataSource):
    # Short identifier
    name = 'mydriver'
    # The type of data that is created from the read method.
    container = 'python'

    def __init__(self, shape, color, **kwargs):
        self._shape = shape
        self._color = color
        self.npartitions = 2
        super().__init__(**kwargs)

    def _get_partition(self, partition):
        print("GET_PARTITION", partition['index'])
        partitions = [self._shape, self._color]
        return partitions[partition['index']]

    # If you want partition to be more complex than an integer, you need to
    # override read. Read is what calls get_partition, then get_partition calls
    # read partition.
    def read(self):
        print("READ")
        return [self._get_partition({'index': i}) for i in range(self.npartitions)]

    # Returns the schema of the container.
    def _get_schema(self):
        return Schema(
            datashape=(2,),
            npartitions=1,
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
