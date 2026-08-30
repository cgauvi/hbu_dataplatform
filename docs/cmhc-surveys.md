# The CMHC surveys

## The vacancy rates

Two assets, and the seam between them is where this pipeline's bronze/silver
line is easiest to see.

`cmhc_vacancy_survey` (bronze) reads CMHC's [Rental Market
Survey](https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/rental-market/urban-rental-market-survey-data-vacancy-rates)
— one workbook a year, every Canadian centre in it — and keeps the slice where
`Province == "Qc"` and `Centre == "Montréal"`, exactly as published: every
quartier the survey prints for that centre, under the survey's own spellings,
subtotal rows included. It is partitioned by **date alone**, because there is
nothing borough-shaped about a workbook — one read per day rather than the same
file re-downloaded and re-parsed once per enabled borough.

`vacancy_rates` (silver) applies the crosswalk to that snapshot:

```
<root>/bronze/cmhc_vacancy_survey/2026-08-20/
└── quartier_vacancy_rates.parquet    # every Montreal-CMA quartier, as published

<root>/silver/vacancy_rates/2026-08-20/VSMPE/
├── vacancy_rates.parquet             # 15 rows: the borough average
└── quartier_vacancy_rates.parquet    # 45 rows: this borough's, relabelled
```

CMHC surveys the Montreal **census metropolitan area** and cuts it into its own
neighborhoods, which do not line up with the boroughs everything else here is
partitioned on. `VSMPE` is three of them, `Outremont` is one, and `PR` is the
borough *plus* Senneville, which CMHC will not split out. The crosswalk is
`CMHC_QUARTIERS` in [partitions.py](../src/urban_rag/partitions.py), a third map
alongside the Spectrum namespaces and the borough codes.

It holds one canonical name per quartier, not one per publication: CMHC
*respells* names between survey years — 2022 prints `South West ~ Sud-Ouest`
where 2023 prints `Sud-Ouest`, and swaps a slash for a hyphen in Pierrefonds'
— so names are matched with case, accents and punctuation collapsed, and the
bilingual half dropped. That is deliberately not fuzzy: two names differing by
a letter still differ, a sheet where two quartiers collapse to the same key is
refused, and a quartier the map names but the snapshot does not publish **fails
the partition** rather than quietly shortening the average.

That refusal is the reason for the split. It is a *silver* failure: the bronze
snapshot lands regardless, so a respelling costs a one-line crosswalk fix and a
re-run of one silver asset against parquet already on disk — where before it
cost the day's scrape and left nothing behind to diagnose it with. The same
applies to `cmhc_rent_survey` and `average_rents`.

Unlike the lots and the buildings, the cut is a **name lookup, not a spatial
join** — the survey publishes rates, not geometry — which makes this the one
borough-partitioned asset with no dependency on `reference_neighborhoods`.

The `Quartier` sheet is a cross-tab: five bedroom classes, each two columns
wide (the rate, then a letter grading its reliability). It is unpivoted to one
row per `dwelling_type` × `bedroom_type`, keyed to stable snake_case
(`apartment_other` × `2_bedroom`, `all` × `all`, …) rather than to the French
labels, because the same survey is published in an English workbook whose
headers read differently.

A rate is stored in **percent as published** — `0.2%` is `0.2`, not `0.002` —
and is null wherever there is none. The two reasons for that are kept apart in
`status`, because they mean different things and neither is an average-able
zero:

| `status` | Sheet | Meaning |
| --- | --- | --- |
| `published` | `1.6%` | A rate, with `reliability` in `a`..`d` |
| `suppressed` | `**` | Measured, withheld for confidentiality or reliability |
| `no_units` | `--` | No dwelling of that class exists in the quartier |

The borough figure is the **unweighted mean** of its quartiers' published
rates. Unweighted because this table publishes rates and nothing to weight them
by — the universe counts live in a different CMHC table — so `num_quartiers`
sits on every row next to `num_quartiers_mapped`, and the two rarely match:
suppression is heavy at this geography. For VSMPE in the 2023 survey, 31 of 45
cells are suppressed and the borough's overall rate is Parc-Extension's alone:

```python
import pandas as pd

rates = pd.read_parquet("data/vacancy_rates/2026-08-20/VSMPE/vacancy_rates.parquet")
rates.loc[
    (rates.dwelling_type == "all") & (rates.bedroom_type == "all"),
    ["vacancy_rate_pct", "num_quartiers", "num_quartiers_mapped", "averaged_quartiers"],
]
#    vacancy_rate_pct  num_quartiers  num_quartiers_mapped  averaged_quartiers
# 14              0.3              1                     3      Parc-Extension
```

Every `dwelling_type` × `bedroom_type` is written whether or not anything was
published for it, so the grid is the same 15 rows for every borough and a
suppressed cell is visible as a row rather than as an absence.

The survey year is **resource config, not a partition dimension** — the survey
is annual and this pipeline's date axis is the scrape date. `survey_year` and
`survey_period` (`octobre 2023`) are written as columns, and the field defaults
to `URBAN_RAG_CMHC_SURVEY_YEAR`, so pointing a run at another year takes:

```powershell
$env:URBAN_RAG_CMHC_SURVEY_YEAR = "2022"
uv run dagster asset materialize --select silver/vacancy_rates --partition "2026-08-20|VSMPE" -m urban_rag.definitions
```

An env var rather than config alone because `--config-json` replaces a
resource's config *wholesale* — set `survey_year` that way and `cache_dir`,
which the code location is what knows, has to be restated with it.

2022 and 2023 are published under the French slug as of this writing; 2024 and
anything before 2022 answer 404. The workbook is cached under
`data/cache/cmhc/`, keyed by filename and shared across every scrape date —
the same posture as the PDF and BDOI caches, since a published survey year is
final.

## The average rents

`average_rents` reads CMHC's HMIP reading-mode page for
`Montreal - Average Rent by Bedroom Type by Neighbourhood`, then applies the
same `CMHC_QUARTIERS` crosswalk as `vacancy_rates`:

```powershell
uv run dagster asset materialize --select silver/average_rents --partition "2026-08-20|VSMPE" -m urban_rag.definitions
```

It writes five borough rows, one per `bedroom_type`, plus the quartier cells
behind the mean:

```
<root>/silver/average_rents/2026-08-20/VSMPE/
├── average_rents.parquet
└── quartier_average_rents.parquet
```

The rent is an unweighted mean of published quartier rents, in dollars as
published. Suppressed `**` cells stay null and drop out of the mean.

### Where the CMHC grid meets the cadastre

Not here, and no longer in a silver asset of its own. There used to be a
`lots_with_vacancy_rates` between `neighborhood_lots` and
`building_lot_intersections`, whose job was to widen CMHC's `dwelling_type` ×
`bedroom_type` grid into `cmhc_*` columns and merge it onto every lot by the
partition's `neighborhood` name. The merge was correct and the placement was
not: the grid rode from there into `rag.lots.attributes` and through both
PostGIS joins without anything reading it, and it made every lot file in the
tree carry sixty survey columns that said nothing about the parcel.

Both surveys are borough figures, so the denormalisation has to happen
somewhere — CMHC publishes no geometry and there is nothing to join on but the
name. It now happens at [the grain that asks the
question](lot-profiles.md), as `vacancy_rates` and `average_rents` jsonb on
`gold.lot_profiles`. What was left of that asset was the geometry repair, and it
moved into `building_lot_intersections`, next to the `ST_Intersection` calls it
exists for.

