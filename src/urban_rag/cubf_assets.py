"""The roll's codebook, snapshot per scrape date.

One bronze asset over one spreadsheet: Annexe 2C.1 of the *Manuel d'évaluation
foncière du Québec*, which is the only published thing that says what
`rl0105a` means. The roll states ``4611`` on a unit and stops there; this says
that is a parking garage.

It is the companion to `urban_rag.role_assets.property_assessment_roll` in the
way `uniformized_property_wealth` is - same ministry, separate publication,
separate cadence - except that this one really is an input rather than a
neighbour: `assessment_units` merges it, and a date whose codebook did not land
is a date whose units carry a code and no text. That is why the two share
`assessment_roll_job` rather than getting a schedule of their own.

Date-partitioned and nothing else. The manual numbers uses for the province and
knows nothing about boroughs, so there is no axis here to cut on - the same
posture `street_network` and the two CMHC surveys take.

**Bronze keeps the sheet, not the codes.** The file written under
`bronze/cubf_use_codes/<YYYY-MM-DD>/` carries the hierarchy rows as well as the
1,260 four-character codes: ``1`` (*RÉSIDENTIELLE*), ``10`` (*LOGEMENT*), ``100``
(*Logement*) and only then ``1000``. Selecting the leaves out of that is silver's
job - see `urban_rag.cubf.use_code_descriptions` - and keeping the headings is
what lets a reader see the classification the codes hang off rather than a flat
list of 1,260 strings.

**What it covers, on the roll it is merged onto.** All 437,192 Montreal units
of the 2026 roll state a four-digit `rl0105a`, and the 2025 edition of the
manual describes 437,184 of them. The eight it does not are five codes - 3190,
3410, 3860, 4815 and 6394 - still in force on a roll filed against an earlier
edition, which is the whole reason the merge in silver is a left join: a
retired code is a null description and a kept property, not a dropped one.
1,259 of the sheet's 1,260 codes carry text; ``9800`` is the exception, a slot
held open for a use not yet named.
"""

from datetime import datetime, timezone

from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from urban_rag.cubf import (
    LISTE_SHEET,
    SOURCE_URL,
    CubfError,
    edition_of,
    read_liste,
    sheet_names,
    use_code_descriptions,
)
from urban_rag.frames import write_frame
from urban_rag.layers import key_prefix
from urban_rag.open_data_assets import GROUP as OPEN_DATA_GROUP
from urban_rag.partitions import date_partitions
from urban_rag.resources import CubfResource, ParquetStore
from urban_rag.storage import clear_parquet, join

#: Shared with `urban_rag.open_data_assets` and `urban_rag.role_assets` rather
#: than restated, for the reason those two share it: this is a public
#: open-data publication read straight off a published URL, and the group is
#: what the Dagster UI sorts it into.
GROUP = OPEN_DATA_GROUP

#: The one file this asset writes, under `bronze/cubf_use_codes/<YYYY-MM-DD>/`.
#: Read back by `urban_rag.role_assets.assessment_units`.
CUBF_FILE = "cubf_use_codes.parquet"


@asset(
    key_prefix=key_prefix("cubf_use_codes"),
    partitions_def=date_partitions,
    group_name=GROUP,
    kinds={"parquet"},
    description=(
        "The MEFQ's codes d'utilisation des biens-fonds - what rl0105a means. "
        f"The {LISTE_SHEET!r} sheet as published, headings and all, under "
        f"bronze/cubf_use_codes/<YYYY-MM-DD>/{CUBF_FILE}: cubf, scian, "
        "description and remarque, where a four-character cubf is a use code "
        "and the narrower rows are the hierarchy above it. Province-wide and "
        "uncached - the workbook has no year in its URL and is reissued "
        "whenever the manual is amended. Source: " + SOURCE_URL
    ),
)
def cubf_use_codes(
    context: AssetExecutionContext, cubf: CubfResource, store: ParquetStore
) -> MaterializeResult:
    scrape_date = context.partition_key
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date)

    fetcher = cubf.fetcher()
    try:
        workbook = fetcher.fetch()
        sheets = sheet_names(workbook)
        frame = read_liste(workbook)
    except CubfError as exc:
        # The whole asset is this one sheet, so a failure here is worth the
        # whole partition - the same posture `reference_neighborhoods` takes
        # on its geographic layer.
        raise Failure(f"{fetcher.url}: {exc}") from exc

    if frame.empty:
        raise Failure(
            f"{fetcher.url}: the {LISTE_SHEET!r} sheet parsed to no rows; the "
            "workbook's layout has changed."
        )

    edition = edition_of(workbook)
    frame["source_file"] = fetcher.url.rsplit("/", 1)[-1]
    frame["source_sheet"] = LISTE_SHEET
    # The only thing that distinguishes two downloads of a file revised in
    # place - see `cubf.edition_of`. Null where the notice was re-worded.
    frame["mefq_edition"] = edition
    frame["scrape_date"] = scrape_date
    frame["scraped_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(frame, join(output_dir, CUBF_FILE))

    # What silver will actually be able to merge, counted here rather than
    # discovered there: a sheet that parsed to 1,700 rows and 3 use codes is a
    # layout change that has not raised, and this is the number that says so.
    descriptions = use_code_descriptions(frame)
    context.log.info(
        "%s: %d row(s) of the %r sheet, %d use code(s) with a description "
        "(MEFQ edition %s) -> %s",
        scrape_date,
        len(frame),
        LISTE_SHEET,
        len(descriptions),
        edition or "unknown",
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_rows": len(frame),
            # The four-character rows, which are the only ones a unit can
            # state. The rest are the hierarchy above them.
            "num_use_codes": len(descriptions),
            # Numbered by the manual and left undescribed. One in the 2025
            # edition - 9800, the slot held open for a use not yet named - and
            # a partition where this climbs is an edition mid-revision.
            "num_codes_without_a_description": _numbered_leaves(frame)
            - len(descriptions),
            "mefq_edition": edition or "unknown",
            # The MAJ<year> change logs, one per amendment since 2010. Not
            # read, and named for the reason `property_assessment_roll` names
            # its unread layers: a run should report what it left rather than
            # leaving nine sheets unaccounted for.
            "sheets_not_read": MetadataValue.json(
                [name for name in sheets if name != LISTE_SHEET]
            ),
            "output_path": MetadataValue.path(str(path)),
            "source_url": MetadataValue.url(SOURCE_URL),
            "download_url": MetadataValue.url(fetcher.url),
        }
    )


def _numbered_leaves(frame) -> int:
    """How many rows carry a four-character `cubf`, described or not."""
    codes = frame["cubf"].astype("string").str.strip()
    return int((codes.str.fullmatch(r"\d{4}").fillna(False)).sum())
