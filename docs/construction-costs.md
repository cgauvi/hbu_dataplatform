# What it costs to build

A zoning envelope says what *may* be built on a lot. It says nothing about
what that would cost, and a highest-and-best-use answer needs both. The cost
side comes from the Altus Group Canadian Cost Guide, as published by the ZEF
construction estimator:

- [zef-builds.github.io/construction-estimator](https://zef-builds.github.io/construction-estimator/),
  a browser tool over the 2026 guide, nine Canadian markets, ~64 building types.

Unlike every other source here it publishes nothing to fetch as data. The cost
table ships as one of the JavaScript files the page loads —
`data/building-types.js`, 16 kB, declaring `CITIES` and a `TYPES` array whose
entries carry a `rates` map of `city -> [low, high]`. That file is the source,
so `urban_rag.estimator` reads it directly rather than driving the page, and
parses the array literals rather than evaluating them: bare keys are quoted and
the result goes through `json.loads`, so a file that grows a function call
fails with its own text in the message instead of running.

Two assets, split the way a proforma asks the question rather than the way the
guide is laid out:

```
bronze/montreal_residential_costs      date   condo/apartment by storey band, and the rest of `residential`
bronze/montreal_nonresidential_costs   date   commercial, industrial, parking
```

Both are **date-partitioned only**. The guide prices nine markets and knows
nothing about boroughs, so there is no borough axis to partition on — the same
posture as `street_network` and the two CMHC surveys. Taking the Montreal
column out of a nine-city table is a bound on what was asked for rather than an
interpretation of what came back, which is what keeps these in bronze.

The storey band is the axis the residential rates actually vary along, and it
is published as part of the label rather than as a number:

| `label` | `rate_low` | `rate_high` |
| --- | ---: | ---: |
| Condominium / Apartment (Up to 12 Storeys) | 275 | 335 |
| Condominium / Apartment (13–39 Storeys) | 320 | 330 |
| Condominium / Apartment (40–60 Storeys) | 330 | 375 |
| Condominium / Apartment (60+ Storeys) | 330 | 425 |
| Wood Frame Condo (Up to 6 Storeys) | 225 | 290 |

Those are dollars per square foot, and the rows arrive in the guide's own
order, which for this family is ascending storeys. **The band stays inside the
label.** Turning `(13–39 Storeys)` into a `storeys_min`/`storeys_max` pair is
reading the label, which is silver's job — see the layer contract in
`urban_rag.layers`. There is no silver asset over these yet; when there is, it
is the place that parse belongs.

## Parking is priced per stall, not per square foot

The one thing to read before reading a rate. `unit_flag` carries whichever
optional key the publisher set on the entry, and is null for everything priced
per square foot:

| `label` | `cat` | `unit_flag` | `rate_low` | `rate_high` |
| --- | --- | --- | ---: | ---: |
| Warehouse | industrial | *(null)* | 120 | 185 |
| Parking – Underground Garage | parking | `perStall` | 51,925 | 68,675 |
| Parking – Above Grade Garage | parking | `perStall` | 38,500 | 57,750 |
| Parking – Surface Lot | parking | `perStall` | 3,960 | 8,250 |

The figures are four orders of magnitude apart, so a reader that ignores the
column will not get a slightly wrong answer — it will get a stall priced as a
square foot. The guide's `institutional` and `infrastructure` categories are
published too and are read by neither asset.

Column names are the publisher's own keys wherever the publisher has one
(`id`, `label`, `sector`, `cat`, `sourceNote`), so a partition lines up against
`data/building-types.js` without a crosswalk. Beside them are the usual
snapshot columns, plus one this source needs more than most:
`source_last_modified` is the `Last-Modified` header the script was served
with — the publisher's own answer to *when did this last change*, for a file
that carries no version stamp of its own.

Run both by hand with `make costs`; they are scheduled at 4:47 alongside the
CMHC surveys, which have no upstream in this pipeline either.

