from intake.catalog.local import Catalog, LocalCatalogEntry
from remote_test.drivers import MyDriver

class InnerCatalog(Catalog):
    def __init__(self, *args, shape, **kwargs):
        self._shape = shape
        super().__init__(*args, **kwargs)

    def _load(self):
        print(f'loaded inner catalog for {self._shape}')
        for color in ('red', 'green', 'blue'):
            self._entries[color] = LocalCatalogEntry(
                name=color,
                driver=MyDriver,
                description='',
                catalog=self,
                args={'shape': self._shape, 'color': color})

class OuterCatalog(Catalog):
    def _load(self):
        for shape in ('circle', 'square', 'triangle'):
            self._entries[shape] = LocalCatalogEntry(
                name=shape,
                driver=InnerCatalog,
                description='',
                catalog=self,
                args={'shape': shape})
