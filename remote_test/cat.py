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
        self.npartitions = 3
        super().__init__(self.npartitions, **kwargs)

    def _get_partition(self, partition):
        print("GET_PARTITION", partition['index'])
        partitions = [self._shape, self._color]
        return partitions[partition['index']]

    # I think, if you want partition to be more complex than an integer,
    # you need to override read because read in the base class calls
    # get_partition with an integer.
    # Some reason this method never gets called when I do this:
    # print(remote_catalog['outer']()['circle']()['green'].read())
    def read(self):
        print("READ")
        return [self._get_partition({'index': i}) for i in range(self.npartitions)]

    # I think read partition only takes an integer, but is able to call
    # get_partition with something more complex than an integer.
    def read_partition(self, i):
        return self._get_partition({'index': i})

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
