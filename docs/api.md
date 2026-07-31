# API reference

Everything below is generated from the type stubs shipped with the package, so
it always matches the installed version. The whole public surface is
importable straight from the top-level `quickhouse` package.

```{eval-rst}
.. currentmodule:: quickhouse
```

## sync

The one call that does the work.

```{eval-rst}
.. autofunction:: sync
```

## Sources

Connection descriptors accepted as ``sync()``'s ``source`` argument.

```{eval-rst}
.. autoclass:: Postgres

.. autoclass:: MySQL

.. autoclass:: BigQuery

.. autoclass:: CleverTap

.. autoclass:: AppsFlyer
```

## Destinations

Connection descriptors accepted as ``sync()``'s ``target`` argument. ``BigQuery``
(above) also works as a destination when constructed with ``dataset_id``.

```{eval-rst}
.. autoclass:: ClickHouse

.. autoclass:: S3Archive
```

## Result & progress types

```{eval-rst}
.. autoclass:: TransferResult
   :members:

.. autoclass:: Progress
   :members:
```

## Data quality

Optional Great Expectations gate passed to ``sync()``'s ``validate=`` — see the
[data-quality guide](guide/data-quality.md). Requires `pip install quickhouse[quality]`.

```{eval-rst}
.. autoclass:: Validation

.. autoexception:: ValidationFailed
```

## Helpers

```{eval-rst}
.. autofunction:: progress_bar

.. autofunction:: version
```

The package also exposes `quickhouse.__version__` (a string), equivalent to the
value returned by {func}`version`.
