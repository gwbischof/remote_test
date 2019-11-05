# Intake nested remote catalog example.

Install
-------

```bash
pip install -e .
```

Test
----

This test starts an intake server and creates a local catalog and a remote
catalog and then checks that the entries can be accessed.  I will extend it
later to check that the entries data can be accessed also (read()).

To run the test:
```bash
pytest
```


