from intake.catalog.local import Catalog, DataSource, LocalCatalogEntry
from intake.source.base import Schema
from intake.container import container_map

class MyDriver(DataSource):
    container = 'python'

    def __init__(self, shape, color, **kwargs):
        self._shape = shape
        self._color = color
        super().__init__(**kwargs)

    def _get_partition(self, partition):
        print(f'fetching data for {(self._shape, self._color)}')
        print("PARTITION", partition)
        return self._shape, self._color

    def _get_schema(self):
        return Schema(
            datashape=(2,),
            npartitions=1,
            extra_metadata={})


class InnerCatalog(Catalog):

    def __init__(self, shape, **kwargs):
        self._shape = shape
        super().__init__(**kwargs)

    def _load(self):
        print(f'loaded inner catalog for {self._shape}')
        for color in ('red', 'green', 'blue'):
            self._entries[color] = LocalCatalogEntry(
                name=color,
                driver='remote_test.cat.MyDriver',
                description='',
                catalog=self,
                direct_access='forbid',
                args={'shape': self._shape, 'color': color})

    def read_partition(self, partition):
        print(partition['index'])


class OuterCatalog(Catalog):

    def _load(self):
        for shape in ('circle', 'square', 'triangle'):
            self._entries[shape] = LocalCatalogEntry(
                name=shape,
                driver='remote_test.cat.InnerCatalog',
                description='',
                catalog=self,
                direct_access='forbid',
                args={'shape': shape})

    def read_partition(self, partition):
        print(partition['index'])
