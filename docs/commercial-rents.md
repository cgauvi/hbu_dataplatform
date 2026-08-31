# What a square foot of commercial floor earns

CMHC prices a dwelling. The Altus cost guide prices a building to *put up*.
Neither says what a square foot of retail, office or warehouse **rents for** —
and until these three assets, that figure was two constants in
[`urban_rag.program`](../src/urban_rag/program.py):

```python
COMMERCIAL_REVENUE_PER_SQFT_CAD = 80.0
INDUSTRIAL_REVENUE_PER_SQFT_CAD = 30.0
```

Both are well above what Montreal actually pays. The measured Q2 2026 figures
are **$36.59** gross for office and **$18.74** gross for industrial, and
neighbourhood retail asks around **$26** gross — so the old commercial constant
overstated a Villeray dépanneur by roughly three times, straight into every
`cap_rate_pct` in [comparables.md](comparables.md).

```bash
make rent-sources DATE=2026-08-26
make commercial-rents DATE=2026-08-26 NEIGHBORHOOD=VSMPE
make commercial-rents DATE=2026-08-26 NEIGHBORHOOD=VSMPE RETAIL_BASE=24.0
```

## Two publishers, because neither is enough alone

| | covers | publishes | free? |
| --- | --- | --- | --- |
| [Cushman & Wakefield MarketBeat](https://www.cushmanwakefield.com/en/canada/insights/canada-marketbeats/montreal-marketbeats) | office, industrial | **levels**, quarterly, by submarket | yes |
| [StatCan CRSPI 18-10-0260-01](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810026001) | office, retail, industrial | an **index**, quarterly, Montreal CMA | yes |

**C&W measure levels but publish no retail report.** There is no free survey of
a Montreal retail *level* anywhere — which is awkward, because retail is most of
the commercial floor in a borough of triplexes and corner shops.

**Statistics Canada cover retail but publish no level.** CRSPI is `2019=100` per
series.

So retail is a **stated base carried forward by the retail index**, and it is
the one rate in the chain with no survey behind it. `RETAIL_BASE` is where that
judgement lives, `source` says `stated_base` on that row, and `rent_basis` never
says `measured` for it.

> **The index is only ever used to move one series through time.**
> `Retail / Office` at a given quarter is how retail has moved *relative to*
> office since 2019 — **not** the ratio of their rents. Using it to turn an
> office level into a retail one would silently assume the two were equal in
> 2019, which they were not, and would produce a confident number wrong by
> whatever the 2019 gap was. `crspi.escalate` takes a single `building_type` for
> exactly that reason, and a test asserts the signature cannot take two.

The index earns its keep twice over: it covers retail, and it fixes a timing
problem nobody would otherwise notice. A MarketBeat lands weeks after the
quarter it describes, so a scrape date in between has a rent measured for a
quarter that is not the one being scraped. `rent_basis` says which happened:

| basis | means |
| --- | --- |
| `measured` | the survey quarter is already the index's latest |
| `escalated` | a surveyed level moved to a later quarter |
| `unescalated` | the index does not reach one end, so the level is unmoved — a rate a quarter stale, said so |
| `stated` | the retail base, which no survey stands behind |

## The URLs cannot be constructed, so they are discovered

The MarketBeat filename changes shape between sectors and between quarters, and
a name that worked last quarter answers **403** this one:

```
2026 Q2 office      montreal-americas-office-marketbeat-q22026.pdf
2026 Q2 industrial  montreal_americas_industrial_marketbeat-q22026.pdf   (underscores)
2026 Q1 office      montreal_americas_office_marketbeat-q12026-.pdf      (trailing dash)
2025 Q4 office      montreal-office-marketbeat-q4-2025.pdf
2025 Q2 industrial  q2-2025-montreal-industrial-marketbeat.pdf           (leading quarter)
```

The **path** is the stable part — every report sits under
`/marketbeat-pdfs/<year>/q<n>/canada/` — so `discover_reports` reads the landing
page, takes the year and quarter off the path and the sector off the filename.
Same posture `linked_documents` takes toward the zoning PDFs: follow the links
the publisher gives rather than inventing them.

C&W is also the only major brokerage that serves the file at all. Colliers and
CBRE both answer 403 to a normal client.

## The borough gets its own rent

Page two of each report carries a submarket table, and
`partitions.MARKETBEAT_SUBMARKETS` says which submarket each borough sits in.
Villeray–Saint-Michel–Parc-Extension is **Midtown North** on both maps:

| | island-wide | Midtown North |
| --- | --- | --- |
| office, gross | $36.59 | **$22.39** |
| industrial, net + additional | $14.06 + $4.68 | **$12.98 + $4.09** |

Pricing a Villeray shop at the island average would overstate it by about 60%.
A borough with no submarket mapped falls back to the whole-market row and says
so in `is_submarket_rate` — a worse answer, reported as such, the same shape
`vacancy_rates` takes when CMHC suppresses a quartier.

## Everything is gross, and getting there differs by sector

`rent_psf_cad` is a **gross** annual rent per square foot on all three rows —
what a tenant pays. The two reports do not quote it the same way, and the gap is
about a quarter of an industrial rent:

- **Office** states one *full service* figure (the footnote says so). Gross
  already.
- **Industrial** states a *direct net* rent with the operating costs beside it in
  `OVERALL WEIGHTED AVG ADDITIONAL RENT`. Gross is the sum, and both halves are
  kept in `published_net_rent_psf_cad` / `published_additional_rent_psf_cad` so
  the arithmetic is checkable from the row.

Reading the industrial net as though it were gross understates a warehouse by a
quarter — a plausible number rather than a crash.

### Parsing the table

Two things about the PDF shape the parser:

**Rows have a variable number of columns.** A submarket with no construction
under way omits those cells, so `Montréal Midtown North` has five numbers before
its rents and `Montréal East` has seven. The parser reads **from the right** —
the trailing money cells are the rents whatever precedes them, and the leading
non-numeric tokens are the name. A column-index parser would take a square
footage as a rent on exactly the rows that omit a column.

**The transaction blocks also end in money.** `Emballages Carrousel 239,825
$34,900,000 / $146` would otherwise land as a $146 submarket rent with a
plausible-looking name. The table is therefore bounded by its own header and
footer — and the window has to start at the header, not merely end at the
footer, because the office report puts its transactions **above** the table and
the industrial one **below**.

## What it changes downstream

The commerce split is the interesting part. The CUBF's 4000s are retail and its
5000s and 6000s are offices and services, and the two rent dollars apart — so
`comparables.rent_class_of` splits them where `income_class_of` does not:

| CUBF | reported as | priced as |
| --- | --- | --- |
| 1000 | residential | residential (CMHC, per dwelling per month) |
| 4000 | commercial | **retail** |
| 5000, 6000 | commercial | **office** |
| 2000, 3000, 7000 | industrial | industrial |
| 8000, unplaceable | none | nothing |

`commercial_floor_area_m2` and `commercial_income_cad` keep exactly the meaning
they had — the two halves added back together — and `retail_*` / `office_*`
travel beside them for anyone who wants the split. Nothing downstream has to
know the split exists unless it wants to.

Every row of `silver.lot_assessment_comparables` and `gold.lot_profiles` carries
`income_assumptions`, which now includes a `rent_provenance` entry per class:

```json
{
  "retail_rent_per_sqft_cad": 26.46,
  "office_rent_per_sqft_cad": 22.39,
  "industrial_rent_per_sqft_cad": 17.07,
  "rent_provenance": {
    "office": "cushman_wakefield_marketbeat 2026-Q2 Midtown North (measured -> 2026-04)",
    "retail": "stated_base 2025-01 island-wide (escalated -> 2026-04)"
  }
}
```

A rate with no provenance beside it cannot be read against next quarter's — the
rule `construction_costs` already follows.

## Where it sits in the day

```
rent-sources       04:47   →  bronze/montreal_commercial_rents
                              bronze/commercial_rent_index
vacancy / rents    05:55/58
lot-values         06:30
commercial-rents   06:10   →  silver.commercial_rents
comparables        06:40   →  prices every square foot against the above
```

`sql/020_silver_commercial_rents.sql` has no `-- requires:` header, so the table
lands on the first `db.py init`.

## What is still stated

Three things, and they are named rather than buried:

- **The retail base** (`RETAIL_BASE`, default $26 gross at 2025-Q1). No free
  survey publishes a Montreal retail level.
- **The three vacancies** netted per class. The MarketBeats publish a *space*
  vacancy for a whole submarket, which is not the credit-loss allowance a
  proforma nets off a rent; conflating them would borrow a number for a job it
  does not do.
- **The operating expense ratio** (`OPEX`, default 0.35), which is now the
  single largest stated lever on any cap rate this platform produces.
