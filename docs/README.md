# Documentation

The [root README](../README.md) is the short version: what this is, how to run
it, and what is blocked. Everything else lives here.

## The platform

| | |
| --- | --- |
| [architecture.md](architecture.md) | The medallion contract each layer owes a reader, the single writer behind every silver and gold table, and the shape of the output tree |
| [assets.md](assets.md) | Every asset, its partitions and its output — plus which ones are blocked on hbu_infra SQL, and what `lot_buildable_setbacks` measures |
| [setup.md](setup.md) | Installing, the certificate traps on a managed laptop, the devcontainer, WSL, the images, S3 output, compose, and Dagster's own storage |
| [running.md](running.md) | Materializing a partition, the schedules, adding a borough, the Feature Service's quirks, and the tests |

## The data

Roughly in lineage order — each page is one source or one join, with the
reasons the code reads it the way it does.

| | |
| --- | --- |
| [cadastre.md](cadastre.md) | Montreal's 91 reference neighborhoods, and the Infolot lots inside a borough |
| [assessment-roll.md](assessment-roll.md) | Quebec's *rôle d'évaluation foncière*, and what every lot is assessed at |
| [comparables.md](comparables.md) | What each lot yields on that assessment, and which lots the roll says are like it |
| [cmhc-surveys.md](cmhc-surveys.md) | Vacancy rates and average rents, and the crosswalk from CMHC's quartiers to boroughs |
| [construction-costs.md](construction-costs.md) | The Altus cost guide's Montreal column — and why parking is priced per stall |
| [lot-documents.md](lot-documents.md) | The spatial join from a lot to the map features covering it, and on to their PDFs |
| [street-frontage.md](street-frontage.md) | The *géobase double*, and how much street each lot actually fronts on |
| [lot-profiles.md](lot-profiles.md) | `gold.lot_profiles` — every lot in the borough, one row each |
| [corpus.md](corpus.md) | The zoning PDFs fetched, chunked and embedded; the DuckDB and pgvector stores that answer against them |
