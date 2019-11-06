import intake
import os
import pytest
import requests
import subprocess
import time
import xarray as xr


PORT = 8425
here = os.path.abspath(os.path.dirname(__file__))
cat_file = os.path.join(here, 'catalog.yml')

@pytest.fixture(scope='module')
def intake_server():
    command = ['intake-server', '-p', str(PORT), cat_file]
    try:
        P = subprocess.Popen(command)
        timeout = 10
        while True:
            try:
                requests.get('http://localhost:{}'.format(PORT))
                break
            except:
                time.sleep(0.1)
                timeout -= 0.1
                assert timeout > 0
        yield 'intake://localhost:{}'.format(PORT)
    finally:
        P.terminate()
        P.communicate()


def test_remote(intake_server):
    cat_local = intake.open_catalog(cat_file)
    cat_remote = intake.open_catalog(intake_server)
    assert 'outer' in cat_remote
    assert 'outer' in cat_local
    print(tuple(cat_remote))
    print(tuple(cat_remote['outer']()))
    print(tuple(cat_remote['outer']()['circle']()))
    print(cat_remote['outer']()['circle']()['green'])
    print(cat_remote['outer']()['circle']()['green'].read())
