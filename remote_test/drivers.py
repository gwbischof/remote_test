from intake.catalog.local import DataSource

class MyDriver(DataSource):
    def __init__(self, *, shape, color, **kwargs):
        self._shape = shape
        self._color = color
        super().__init__(**kwargs)
    def _get_partition(self, partition):
        print('fetching data for {(self._shape, self_color)}')
        return self._shape, self._color
