'''Manipulate Delta Tables Lab This notebook provides a hands - on review of some of the more esoteric features Delta Lake brings to the data lakehouse.Learning Objectives By the
end of this lab,
you should be able to: Review table history Query previous table versions
and rollback a table to a specific version Perform file compaction
and Z - order indexing Preview files marked for permanent deletion
and commit these deletes'''

DESCRIBE HISTORY beans

CREATE
OR REPLACE TEMP VIEW pre_delete_vw AS (
    SELECT
        *
    FROM
        beans VERSION AS OF 4
)

RESTORE TABLE beans VERSION AS OF 5

OPTIMIZE beans ZORDER BY (name);