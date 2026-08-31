"""Quebec's *richesse foncière uniformisée*, read for the factor inside it.

The assessment roll says what a property is worth **on the roll**, and every
roll is stale by construction: Montreal's is triennial and values every unit
as of one reference date (2024-07-01 for the 2026-2028 roll, which the roll
itself carries as `dat_cond_mrche`). Carrying a roll figure to a market one
takes the *facteur comparatif* - the inverse of the roll's median proportion,
which the municipal evaluator establishes each year by comparing **actual
sales on the territory** against the values entered in the roll's first year,
and which the MAMH approves.

That factor is the only sales-derived number about Montreal's market that is
published openly, per municipality, under a licence this pipeline can use. It
is not in the roll - `urban_rag.comparables` says so where it defaults
`MARKET_FACTOR` to 1.0 - and it is not published on its own either. It is a
*column* of the RFU, the standardized property wealth MAMH computes to compare
municipalities' capacity to raise taxes: every RFU equation is an assessment
total multiplied by `CSALX02163`, and that multiplier is the factor.

So this module reads a fiscal publication for one of its columns, and the
asset over it snapshots the whole file rather than that column alone - bronze
keeps what the publisher sent.

Deliberately free of Dagster imports, like `urban_rag.open_data`, so the
year resolution and the file-picking can be exercised from a plain test.
"""

from __future__ import annotations

import os
import re

#: Données Québec's CKAN, which is where MAMH catalogues the RFU. A different
#: portal from `urban_rag.open_data.DEFAULT_BASE_URL` (the *city's*) but the
#: same CKAN API, so `CkanClient` reaches both - only the base URL changes.
#: Note the `/recherche` segment: the API lives under it, not at the root.
DEFAULT_BASE_URL = "https://www.donneesquebec.ca/recherche"

#: Portal slug of https://www.donneesquebec.ca/recherche/dataset/richesse-fonciere-uniformisee
RFU_DATASET = "richesse-fonciere-uniformisee"

#: Environment variable naming the fiscal year to read. Unset means the latest
#: year the dataset publishes, resolved from the catalogue - see
#: `default_rfu_year`.
RFU_YEAR_VAR = "URBAN_RAG_RFU_YEAR"

#: `CSALX02163`, FACTEUR COMPARATIF - the multiplier every RFU equation applies
#: to an assessment total to standardize it. This is the column the whole asset
#: exists for; `urban_rag.comparables.MARKET_FACTOR` is the knob it feeds.
COMPARATIVE_FACTOR_COLUMN = "CSALX02163"

#: `CIALX02140`, RICHESSE FONCIÈRE UNIFORMISÉE - the publication's headline
#: total, carried for reading rather than read by anything here.
RFU_TOTAL_COLUMN = "CIALX02140"

#: The five-digit geographic code of an *organisme municipal*. Identical to the
#: roll's `code_mun` - Montreal is `66023` in both - which is what lets the
#: factor reach `silver.assessment_units` without a crosswalk.
GEO_CODE_COLUMN = "cod_geo"

#: Column holding the organisme's name, kept for the metadata a run reports.
ORGANISM_NAME_COLUMN = "nom_organisme"

#: Ville de Montréal. The same code `urban_rag.role_assets` filters the
#: province-wide roll down to.
MONTREAL_GEO_CODE = "66023"

#: The data file for a fiscal year. The publisher has changed the case of this
#: name between years - `rfu-2023.csv` against `RFU-2025.csv` - so it is matched
#: case-insensitively rather than written out, and the year is read back off the
#: match instead of being assumed.
_DATA_FILE = re.compile(r"^rfu-(\d{4})\.csv$", re.IGNORECASE)

#: The companion that names what each `CIALX*`/`CSALX*` code means. Its own name
#: has drifted further than the data file's - `rfu-2023-postes.csv` against
#: `RFU-2025-DescriptionPoste.csv` - so only the stem is pinned and whatever
#: follows the year is accepted. The anchored `$` keeps `.xlsx` out; the dataset
#: publishes both formats of both files.
_POSTES_FILE = re.compile(r"^rfu-(\d{4})-[^.]+\.csv$", re.IGNORECASE)


class RfuError(RuntimeError):
    """The dataset does not publish what was asked of it."""


def default_rfu_year() -> int | None:
    """`RFU_YEAR_VAR` if it holds a year, ``None`` for "the latest published".

    ``None`` rather than a constant, unlike `urban_rag.role_foncier.
    default_roll_year`: the roll's archive is addressed by a URL built from the
    year, so a year has to be chosen before anything is fetched, while the RFU
    is resolved out of a catalogue that already lists the years it has. Pinning
    one here would be a number that goes stale every spring and whose staleness
    shows up as a silently old factor rather than as a 404.

    Read per instantiation rather than at import, so a `.env` loaded later - or
    a variable set for one run - still reaches the resource.
    """
    raw = os.environ.get(RFU_YEAR_VAR)
    if raw is None or not raw.strip():
        return None
    try:
        return int(raw)
    except ValueError:
        raise RfuError(f"{RFU_YEAR_VAR}={raw!r} is not a year") from None


def published_years(filenames: list[str]) -> dict[int, str]:
    """Fiscal year -> data filename, for every year the dataset publishes."""
    found: dict[int, str] = {}
    for name in filenames:
        match = _DATA_FILE.match(name)
        if match:
            found[int(match.group(1))] = name
    return found


def pick_data_file(filenames: list[str], year: int | None = None) -> tuple[int, str]:
    """The RFU data file to read, and the year it is for.

    ``year`` of ``None`` takes the latest published, which is what a scheduled
    run wants: the factor is re-established annually and the newest one is the
    one that carries a roll figure closest to today's market.
    """
    published = published_years(filenames)
    if not published:
        raise RfuError(
            f"{RFU_DATASET!r} publishes no rfu-<year>.csv; it has: "
            f"{', '.join(sorted(filenames)) or '(nothing)'}"
        )
    if year is None:
        chosen = max(published)
        return chosen, published[chosen]
    if year not in published:
        raise RfuError(
            f"{RFU_DATASET!r} publishes no RFU for {year}; it has "
            f"{', '.join(str(y) for y in sorted(published))}"
        )
    return year, published[year]


def pick_postes_file(filenames: list[str], year: int) -> str | None:
    """The field-description companion for ``year``, or ``None`` if absent.

    ``None`` rather than a raise: the descriptions are documentation, and a
    year that ships without them is still a year whose factor is readable.
    """
    for name in filenames:
        match = _POSTES_FILE.match(name)
        if match and int(match.group(1)) == year:
            return name
    return None
