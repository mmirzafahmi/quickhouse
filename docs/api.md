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

## reconcile_keys

Measure — and optionally repair — the drift between a source and a destination
synced from it. An incremental sync never removes anything, so a row the source
hard-deletes stays in the destination forever; this is what converges the two.

```{eval-rst}
.. autofunction:: reconcile_keys
```

## Sources

Connection descriptors accepted as ``sync()``'s ``source`` argument.

```{eval-rst}
.. autoclass:: Postgres

.. autoclass:: MySQL

.. autoclass:: BigQuery

.. autoclass:: ClickHouse

.. autoclass:: CleverTap

.. autoclass:: AppsFlyer
```

## DataFrames

Writing an in-memory pandas/polars/pyarrow frame. See the
[DataFrames guide](guide/sources/dataframes.md). Requires
`pip install quickhouse[pandas]`.

```{eval-rst}
.. autofunction:: from_pandas

.. autoclass:: QuickhouseWarning
```

## Destinations

Connection descriptors accepted as ``sync()``'s ``target`` argument. ``BigQuery``
(above) also works as a destination when constructed with ``dataset_id``, and
``ClickHouse`` (above) works in either role.

```{eval-rst}
.. autoclass:: S3Archive
```

## Result & progress types

```{eval-rst}
.. autoclass:: TransferResult
   :members:

.. autoclass:: TransferWarning
   :members:

.. autoclass:: ReconcileResult
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
