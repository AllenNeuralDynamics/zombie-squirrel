"""Cell-by-everything cache tables: one row per cell, across every data asset.

Three tables, joined on ``cell_key``:

* ``cell_index`` -- provenance and identity, one narrow unpartitioned table;
* ``cell_properties`` -- wide sparse per-cell measurements, partitioned by asset;
* ``cell_genes`` -- transcriptomic genotyping, partitioned by subject.

They are built mainly by projecting per-cell tables the sync pipeline already
caches; a source with no pipeline-built table of its own reads NWB-Zarr directly
(``nwb_units.py``) rather than depending on a one-off script-built table. To add a
modality, project or property, see the ``cell-by-everything`` skill and
``sources.py``.
"""

from biodata_cache.cache_table_helpers.cell_by_everything.tables import (  # noqa: F401
    build_cell_by_everything,
    cell_genes,
    cell_genes_columns,
    cell_index,
    cell_index_columns,
    cell_properties,
    cell_properties_columns,
)
