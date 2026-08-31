"""What a lot yields on what it is assessed at, and which lots are like it.

Two calculations over one frame of lots, kept together because the second is
what makes the first readable. `lot_assessed_values` answers *what is the
property on this lot assessed at*; nothing downstream of it answers *is that a
lot of money for this lot*, and a cap rate is one of the two ways to ask. The
other is a comparable, and this module computes both.

Deliberately free of Dagster imports, the same posture `urban_rag.role_foncier`
and `urban_rag.program` take: everything here is arithmetic over frames, and
the asset that calls it is `urban_rag.comparables_assets`.

**A cap rate is income over value, and the roll supplies only the value.** The
income side is built from what the roll says stands on the lot - dwellings,
floor area, and the CUBF use code that says what the floor is *for* - priced
against CMHC's borough rents for the dwellings and, for everything else,
against the surveyed rents `silver/commercial_rents` resolves for the borough:
Cushman & Wakefield's office and industrial levels for the submarket the
borough sits in, and a stated retail base, all carried to the current quarter
by Statistics Canada's rent index. `urban_rag.program`'s constants remain the
default, so a caller that hands over no rates still computes - but a partition
that went through the asset carries measured numbers, and `rent_provenance`
says for each one which publisher, which quarter and whether it was measured,
escalated or stated.

**Commerce is priced in two halves and reported as one.** The CUBF's 4000s are
retail and its 5000s and 6000s are offices and services, and the two rent for
about $4 a square foot apart in Montreal - so `rent_class_of` splits them,
`annual_income` charges each at its own rate, and `commercial_income_cad` is
the sum. Every column a reader downstream already had keeps its meaning; the
split shows up as two extra floor and income columns beside them.

**The floor is split by the unit's own use code, not by the lot's.** A unit is
one *unite d'evaluation* with one `rl0105a`, so the class its whole floor area
belongs to is a property of the row rather than a judgement about the parcel -
`CUBF_CLASSES` is the whole of the mapping, and a lot carrying a triplex over a
depanneur gets both a residential and a commercial income because its two units
say so. `dominant_use_code` exists for reading, not for the arithmetic.

**Every characteristic is counted whole, and that is what makes the ratio
safe.** A unit spanning several lots has its value counted whole on each of
them in `total_assessed_value` - see `urban_rag.role_assets` - and this module
counts its dwellings and floor area the same way. That over-counts a borough on
either side of the fraction and therefore cancels in the quotient: a shared
triplex contributes three dwellings and its whole value to both lots, and both
lots report the yield of a triplex. The apportioned total is the column to sum
across lots; the cap rate is not a column to sum at all.

**Assessed value is not market value.** Quebec's roll is triennial and every
unit in it is valued as of one reference date, and the province publishes a
*facteur comparatif* per municipality to carry a roll figure to a market one.
That factor is not in this publication, so it is `market_value_factor` on
`IncomeAssumptions` - default 1.0, which reports the cap rate *on the roll* and
says so - rather than a number invented here.

----------------------------------------------------------------------------
The comparables
----------------------------------------------------------------------------

`nearest_comparables` finds, for every lot, the ``k`` lots most like it on four
things at once: what it is used for, how big the parcel is, how much floor
stands on it and how many dwellings that floor holds - and how far away it is.

**One weighted distance rather than a filter and a sort.** Each feature becomes
a dimensionless distance scaled by what a unit of it is worth being wrong
about, and the composite is the weighted Euclidean norm over them::

    d(i,j) = sqrt( sum_f  w_f * d_f(i,j) ** 2 )

so `ComparableWeights` states both halves of every judgement in one place - the
scale that turns metres or a size ratio into a comparable number, and the
weight that says how much that number counts. A metric distance of 500 m, a
factor of two in floor area and a use code from a different CUBF class are all
one unit apart by default, which is the claim the defaults are making and the
first thing to move when the neighbour lists look wrong.

**Size is logged, distance is not.** Twice the floor area is the same
difference in kind whether it is 100 m2 against 200 or 1,000 against 2,000, and
a linear scale on a quantity that spans four orders of magnitude would make
every small lot equidistant from every other. Ground distance is the opposite:
300 m is 300 m anywhere in the borough, and a log would make the far side of
the parcel and the far side of the borough look alike.

**The use code is categorical and is scored in three steps.** Same four-digit
`rl0105a` is 0 - the roll says these are the same kind of thing. The same CUBF
class is `same_class_penalty`, because a duplex and a triplex are both
Habitation and genuinely comparable. Anything else is `different_class_penalty`.
Nothing here reads the code as a magnitude: 1000 and 4000 are not three
thousand apart, they are residential and commercial.

**A feature neither side can state costs `missing_penalty`.** A lot no
assessment unit stands on has no floor area, no dwelling count and no use code -
not a zero of each - so those components are neither dropped (which would make
a lane the nearest neighbour of every lane) nor read as zero (which would make
it the nearest neighbour of every empty warehouse). It is a stated cost, and it
is why an unassessed lot's comparables come out chosen mostly on size and
distance, which is all that is actually known about it.

**Candidates carry a value; subjects need not.** Every lot in the partition
gets a neighbour list, but only the lots the roll reached can *be* in one: a
comparable with no assessed value contributes no dollars per dwelling and no
dollars per square metre, which is the only thing a neighbour is consulted for.
That asymmetry is the point - it is what lets a vacant parcel be valued off the
built ones around it.

**The pool is this borough.** `lot_assessed_values` is partitioned by borough,
so the lots with values attached are the ones in the partition being computed,
and a parcel on the boundary draws its comparables from its own side of it.
That is a limitation of how the upstream is partitioned rather than a modelling
choice, and `num_candidates` travels in the metadata so a thin pool is visible
rather than inferred.

**The search is exact and quadratic, in chunks.** No tree and no spatial
pre-filter: the composite metric is not the one a KD-tree would index, and
pruning on ground distance alone would silently drop the identical triplex two
streets over in favour of the warehouse next door. `_CHUNK_ROWS` subjects are
scored against every candidate at a time, which holds the working set to a few
tens of megabytes at borough scale. `max_distance_m` bounds the *answer* and
not the work - a comparable across the island is not a comparable - and a lot
with nothing inside that radius reports an empty list rather than a far one.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from urban_rag.program import (
    ASSUMED_BUILDING_AGE_YEARS,
    COMMERCIAL_REVENUE_PER_SQFT_CAD,
    COMMERCIAL_VACANCY_PCT,
    INDUSTRIAL_REVENUE_PER_SQFT_CAD,
    INDUSTRIAL_VACANCY_PCT,
    M2_PER_SQFT,
    MAINTENANCE_PREMIUM_PER_YEAR,
    MAX_MAINTENANCE_PREMIUM,
    MONTHS_PER_YEAR,
    building_age_years,
    effective_operating_expense_ratio,
)
from urban_rag.role_foncier import (
    DWELLINGS_COLUMN as _DWELLINGS,
)
from urban_rag.role_foncier import (
    FLOOR_AREA_COLUMN as _FLOOR_AREA,
)
from urban_rag.role_foncier import (
    LAND_AREA_COLUMN as _LAND_AREA,
)
from urban_rag.role_foncier import (
    NONRESIDENTIAL_UNITS_COLUMN as _NONRESIDENTIAL,
)
from urban_rag.role_foncier import (
    RENTAL_ROOMS_COLUMN as _RENTAL_ROOMS,
)
from urban_rag.role_foncier import (
    STOREYS_COLUMN as _STOREYS,
)
from urban_rag.role_foncier import (
    USE_CODE_COLUMN as _USE_CODE,
)
from urban_rag.role_foncier import (
    VALUE_COLUMN as _VALUE,
)
from urban_rag.role_foncier import (
    YEAR_BUILT_COLUMN as _YEAR_BUILT,
)

#: The unit columns `aggregate_units_by_lot` carries across from the
#: characteristics table. Aliased to short private names on the way in - the
#: aggregation below reads as arithmetic over dwellings and floor area rather
#: than over `rl0311a` and `rl0308a`, and `urban_rag.role_foncier` stays the
#: one place the MAMH codes are written down.
_CARRIED_COLUMNS: tuple[str, ...] = (
    _USE_CODE,
    _LAND_AREA,
    _STOREYS,
    _YEAR_BUILT,
    _FLOOR_AREA,
    _DWELLINGS,
    _NONRESIDENTIAL,
    _RENTAL_ROOMS,
    _VALUE,
)

#: Of those, the ones that are quantities. Coerced with `to_numeric` on the way
#: into the aggregation because the GeoPackage types several as text - a sum
#: over strings either concatenates them or raises, and neither is a floor
#: area. `_USE_CODE` is deliberately absent: it is a classification, and
#: turning it into a number is exactly the mistake `use_class_of` exists to
#: prevent.
_NUMERIC_CARRIED: tuple[str, ...] = (
    _LAND_AREA,
    _STOREYS,
    _YEAR_BUILT,
    _FLOOR_AREA,
    _DWELLINGS,
    _NONRESIDENTIAL,
    _RENTAL_ROOMS,
    _VALUE,
)

#: What the per-lot sums are called once they leave the roll's vocabulary, in
#: the order the output frame carries them. `roll_land_area_m2` keeps the
#: `roll_` prefix because it is *not* the lot's area: a divided co-ownership
#: states the whole parcel on every apartment, so this column sums the same
#: ground once per unit and the polygon's own `lot_area_m2` is the measurement
#: to compare lots on. Carried anyway, because for an ordinary single-unit lot
#: the two agreeing is a useful thing to be able to check.
_SUMMED_COLUMNS: Mapping[str, str] = {
    _DWELLINGS: "num_dwellings",
    _NONRESIDENTIAL: "num_nonresidential_units",
    _RENTAL_ROOMS: "num_rental_rooms",
    _FLOOR_AREA: "floor_area_m2",
    _LAND_AREA: "roll_land_area_m2",
    "residential_floor_area_m2": "residential_floor_area_m2",
    "commercial_floor_area_m2": "commercial_floor_area_m2",
    "industrial_floor_area_m2": "industrial_floor_area_m2",
    # The commerce split, additive: `commercial_floor_area_m2` above still
    # means what it did and these two say what it is made of. They exist
    # because the two halves are priced at different surveyed rents - see
    # `rent_class_of` - and carrying them makes the income reproducible from
    # the row rather than only from the code that wrote it.
    "retail_floor_area_m2": "retail_floor_area_m2",
    "office_floor_area_m2": "office_floor_area_m2",
}

#: NAD83 / MTM zone 8 - the projected CRS this platform measures Montreal in.
#: The same SRID `urban_rag.postgis` computes frontage and setbacks in, named
#: here rather than repeated as a number so the centroid distances below are in
#: the same metres those are.
METRIC_CRS = "EPSG:32188"

#: The CUBF's top-level classes, keyed by the leading digit of `rl0105a`, and
#: the income class each is priced as. The *Manuel d'evaluation fonciere du
#: Quebec* numbers its use codes so that the first digit is the category and
#: the remaining three are the kind of thing within it, which is why
#: `use_class_of` reads one character and never compares two codes as numbers.
#:
#: The mapping to three income classes is this module's own and is a judgement:
#: transport, communication and public utilities (3000) and natural-resource
#: production (7000) are priced as industrial floor because that is what they
#: are built as, and culture and recreation (6000) as commercial for the same
#: reason. Vacant land and water (8000) is priced as nothing at all - there is
#: no floor on it to earn anything, and the roll's own floor area for those
#: units is null or zero.
#:
#: The third element is the **rent class**, and it is finer than the income
#: class on purpose. `silver/commercial_rents` prices retail and office
#: separately - Cushman & Wakefield survey a Montreal office rent and nobody
#: surveys a retail one - and the two are not close: Midtown North office asks
#: about $22 gross where neighbourhood retail asks about $26. Charging a
#: dépanneur an office rent, or the reverse, is a category error worth one
#: extra column to avoid. The *reported* floor columns stay at the three income
#: classes, so `commercial_floor_area_m2` still means what it did; the rent
#: class only decides which rate that floor is multiplied by.
CUBF_CLASSES: Mapping[str, tuple[str, str, str]] = {
    "1": ("habitation", "residential", "residential"),
    "2": ("industrie_manufacturiere", "industrial", "industrial"),
    "3": ("transport_communication_services_publics", "industrial", "industrial"),
    "4": ("commerciale", "commercial", "retail"),
    "5": ("services", "commercial", "office"),
    "6": ("culturelle_recreative_loisirs", "commercial", "office"),
    "7": ("production_extraction_ressources", "industrial", "industrial"),
    "8": ("immeubles_non_exploites_etendues_eau", "none", "none"),
}

#: What `use_class_of` answers for a code it cannot place - a null `rl0105a`, a
#: 9000-series code the manual above does not number, or a value that is not
#: four digits at all. Kept distinct from 8000's "nothing stands here": one is
#: a parcel known to carry no floor, the other is a parcel whose use was not
#: read, and the comparables score the two differently.
UNKNOWN_USE_CLASS = "unknown"

#: The income class an unplaceable use code is priced as: none. A floor whose
#: purpose the roll did not state is not priced at the average of the ones it
#: might have been - `num_lots_with_unknown_use` is the count that says how
#: much floor that leaves unpriced.
UNKNOWN_INCOME_CLASS = "none"

#: The three classes floor area is *reported* under, and the order every
#: payload and every column group lists them in. These are the
#: `*_floor_area_m2` columns and the `*_income_cad` columns downstream reads.
INCOME_CLASSES: tuple[str, ...] = ("residential", "commercial", "industrial")

#: The classes floor area is *priced* under. Finer than the income classes by
#: one split - commerce into retail and office - because that is the grain
#: `silver/commercial_rents` surveys at. `commercial_income_cad` is the two put
#: back together, so nothing downstream has to know the split exists unless it
#: wants to.
RENT_CLASSES: tuple[str, ...] = ("residential", "retail", "office", "industrial")

#: The rent classes whose floor area is charged per square foot per year, in
#: the order `annual_income` sums them. Residential is absent: CMHC prices a
#: dwelling per month, not a square foot, and that is a different arithmetic.
NON_RESIDENTIAL_RENT_CLASSES: tuple[str, ...] = ("retail", "office", "industrial")

#: How `estimated_value_cad` was arrived at, in the order the basis is chosen.
#: Dollars per dwelling first where the subject has dwellings - it is the ratio
#: a residential comparable is actually quoted on - then per square metre of
#: floor, then per square metre of *ground*, which is the only one available
#: for a parcel carrying nothing at all and is what values a vacant lot off the
#: built ones around it.
VALUE_BASES: tuple[str, ...] = (
    "per_dwelling",
    "per_floor_area",
    "per_land_area",
    "none",
)

#: How many subject rows are scored against the whole candidate pool at once.
#: The distance matrix a chunk builds is `_CHUNK_ROWS x num_candidates` float64
#: per feature, so 512 against a borough's ~22,000 valued lots is about 90 MB
#: live at a time - small enough to sit inside an asset that already holds the
#: roll, large enough that the per-chunk numpy overhead disappears.
_CHUNK_ROWS = 512

#: Below this, a ratio is not a ratio. Dollars per dwelling over zero dwellings
#: and dollars per square metre over a parcel of no area are both division by
#: something that is not there, and the row is dropped from the median rather
#: than contributing an infinity to it.
_MIN_DENOMINATOR = 1e-9


def use_class_of(code) -> str:
    """The CUBF class ``code`` belongs to, or `UNKNOWN_USE_CLASS`.

    Reads the leading digit and nothing else, because that is what the code
    means: `rl0105a` is a classification whose first character is the category,
    and 1000 and 4000 are residential and commercial rather than three thousand
    apart. A code that is not four digits is not placed - the roll publishes a
    handful of blanks, and guessing at one would put a parcel in a class the
    publisher never claimed for it.
    """
    entry = _cubf_entry(code)
    return entry[0] if entry else UNKNOWN_USE_CLASS


def income_class_of(code) -> str:
    """Which of `residential` / `commercial` / `industrial` reports ``code``.

    ``'none'`` for vacant land, for water, and for a code that cannot be
    placed: floor area under one of those earns nothing here rather than being
    priced at the average of what it might have been.
    """
    entry = _cubf_entry(code)
    return entry[1] if entry else UNKNOWN_INCOME_CLASS


def rent_class_of(code) -> str:
    """Which surveyed rent prices ``code`` - the income class, commerce split.

    `income_class_of` answers what a lot's floor is *reported* as and this
    answers what it is *charged*, and they differ on exactly one thing: a
    commercial floor is retail or office depending on whether the CUBF puts it
    in 4000 or in 5000/6000. `silver/commercial_rents` surveys those two apart
    because the publishers do, and about $4 a square foot separates them.
    """
    entry = _cubf_entry(code)
    return entry[2] if entry else UNKNOWN_INCOME_CLASS


def _cubf_entry(code) -> tuple[str, str, str] | None:
    if code is None or (isinstance(code, float) and math.isnan(code)):
        return None
    text = str(code).strip()
    if len(text) != 4 or not text.isdigit():
        return None
    return CUBF_CLASSES.get(text[0])


@dataclass(frozen=True)
class IncomeAssumptions:
    """Everything the cap rate needs that the assessment roll does not publish.

    One object rather than eight arguments, and frozen, because every one of
    these is a *stated assumption* and the rule this platform follows for those
    is that the row records the value that produced it. `as_metadata` is what
    travels into the parquet and the jsonb, so a cap rate can always be read
    back against the assumptions behind it - the same rule `max_built_area_m2`
    and `frontage_buffer_m` follow.

    ``average_rent_cad`` and ``vacancy_rate_pct`` are CMHC's, for the borough,
    and are the two that are measured. The rest are not:

    ``operating_expense_ratio`` is the share of gross income that never reaches
    the owner - taxes, insurance, management, maintenance - **for a building
    that is new**. 0.35 is the conventional figure for a Montreal walk-up and
    is the single largest lever on every cap rate this module produces. Vacancy
    is *not* in it: that is netted out of the gross separately, per class, so
    the two are not applied to each other twice.

    It is the *base* of the ratio rather than the whole of it. Only maintenance
    among those four depends on how old the building is, and charging a 1910
    triplex and a building finished this year the same share of rent for the
    roof and the risers is the assumption that most flatters standing stock
    against redeveloping it. ``maintenance_premium_per_year``,
    ``max_maintenance_premium`` and ``assumed_building_age_years`` are the
    curve that adds to it - all three stated, all three
    `urban_rag.program`'s - and ``income_reference_year`` is the year ages are
    taken against, which the asset sets from its own partition key.

    **The age adjustment is off unless ``income_reference_year`` is set.**
    Without a year there is no age, and inventing one from the wall clock would
    make a partition's cap rates depend on the day it was materialized rather
    than on the date it is keyed by. Off, every lot is charged the base and
    `effective_operating_expense_ratio` equals it, which is what this module
    did before the curve existed.

    ``retail_rent_per_sqft_cad``, ``office_rent_per_sqft_cad`` and
    ``industrial_rent_per_sqft_cad`` are **gross annual rents per square
    foot**, and are the three `silver/commercial_rents` resolves for the
    borough - Cushman & Wakefield's surveyed office and industrial levels for
    the submarket the borough sits in, and a stated retail base, all carried to
    the current quarter by Statistics Canada's index. They default to
    `urban_rag.program`'s constants so a caller that hands over nothing still
    computes, but a partition that went through the asset carries measured
    numbers and `rent_provenance` says which.

    The old ``commercial_rent_per_sqft_cad`` is gone. It priced retail and
    office at one rate, and the two are about $4 a square foot apart in
    Montreal - `rent_class_of` is where the split now lives.

    ``retail_vacancy_pct`` and its two siblings are still stated: the
    MarketBeats publish a vacancy but it is a *space* vacancy for a whole
    submarket, which is not the credit-loss allowance a proforma nets off a
    rent, and conflating them would be borrowing a number for a job it does not
    do.

    ``market_value_factor`` multiplies the assessed value before it becomes the
    denominator. 1.0 reports the yield *on the roll*, which is the honest
    default because the province's *facteur comparatif* is not in this
    publication; a reader who knows the year's factor sets it and gets a market
    cap rate.
    """

    average_rent_cad: float | None = None
    vacancy_rate_pct: float | None = None
    operating_expense_ratio: float = 0.35
    maintenance_premium_per_year: float = MAINTENANCE_PREMIUM_PER_YEAR
    max_maintenance_premium: float = MAX_MAINTENANCE_PREMIUM
    assumed_building_age_years: float = ASSUMED_BUILDING_AGE_YEARS
    income_reference_year: int | None = None
    retail_rent_per_sqft_cad: float = COMMERCIAL_REVENUE_PER_SQFT_CAD
    office_rent_per_sqft_cad: float = COMMERCIAL_REVENUE_PER_SQFT_CAD
    industrial_rent_per_sqft_cad: float = INDUSTRIAL_REVENUE_PER_SQFT_CAD
    retail_vacancy_pct: float = COMMERCIAL_VACANCY_PCT
    office_vacancy_pct: float = COMMERCIAL_VACANCY_PCT
    industrial_vacancy_pct: float = INDUSTRIAL_VACANCY_PCT
    market_value_factor: float = 1.0
    survey_year: int | None = None
    survey_period: str | None = None
    #: One entry per rent class, as `silver/commercial_rents` resolved it:
    #: which publisher, which quarter, which submarket, and whether the figure
    #: was measured, escalated or stated. Empty when the caller supplied no
    #: surveyed rates, which is what says the defaults above are in force.
    rent_provenance: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not 0.0 <= self.operating_expense_ratio < 1.0:
            raise ValueError(
                "operating_expense_ratio is a share of gross income and must "
                f"be in [0, 1), got {self.operating_expense_ratio!r}"
            )
        if self.maintenance_premium_per_year < 0:
            raise ValueError(
                "maintenance_premium_per_year is added to the base ratio with "
                f"age and cannot be negative, got {self.maintenance_premium_per_year!r}"
            )
        if self.max_maintenance_premium < 0:
            raise ValueError(
                "max_maintenance_premium is a share of gross income and "
                f"cannot be negative, got {self.max_maintenance_premium!r}"
            )
        if self.assumed_building_age_years < 0:
            raise ValueError(
                "assumed_building_age_years is an age in years and cannot be "
                f"negative, got {self.assumed_building_age_years!r}"
            )
        if self.market_value_factor <= 0:
            raise ValueError(
                "market_value_factor scales the assessed value and must be "
                f"positive, got {self.market_value_factor!r}"
            )

    @property
    def has_residential_rent(self) -> bool:
        """Whether CMHC published a borough rent at all.

        False is a real answer and a common one - CMHC suppresses the whole
        grid for a borough it surveyed too few units in - and it is why the
        residential income below is null rather than zero when it happens. A
        borough with no published rent and no commercial floor gets a null cap
        rate, which is the difference between "not surveyed" and "earns
        nothing".
        """
        return self.average_rent_cad is not None and self.average_rent_cad > 0

    @property
    def residential_occupancy(self) -> float:
        """The share of a dwelling-year that is actually collected.

        `vacancy_rates` stores a rate as published - 0.2 percent is ``0.2`` -
        so this divides by 100. Reading that column as a fraction would
        understate revenue by two orders of magnitude and produce a confident
        answer rather than an exception, which is the same trap
        `urban_rag.program` warns about at the top of its own module. A borough
        CMHC suppressed the vacancy for is fully occupied here rather than
        fully empty: the rent is what was measured, and inventing a vacancy for
        it would move the answer further than assuming none.
        """
        return _occupancy(self.vacancy_rate_pct)

    def expense_ratios(self, year_built: pd.Series) -> pd.DataFrame:
        """Per-lot age, maintenance premium and effective expense ratio.

        Three columns rather than one, because the ratio a row was charged is
        not readable back from the ratio alone: 0.43 is a 1955 building at the
        default curve and a 1990 one at a steeper one, and the assumptions
        object says which curve without saying which age.

        With no `income_reference_year` there is no age to charge, so the
        premium is zero everywhere and the effective ratio is the base - see
        the class docstring. `building_age_years` is null in that case rather
        than zero: the lots did not all become new, the question was not asked.
        """
        index = year_built.index
        if self.income_reference_year is None:
            return pd.DataFrame(
                {
                    "building_age_years": pd.Series(np.nan, index=index, dtype="float64"),
                    "maintenance_premium": pd.Series(0.0, index=index, dtype="float64"),
                    "effective_operating_expense_ratio": pd.Series(
                        self.operating_expense_ratio, index=index, dtype="float64"
                    ),
                },
                index=index,
            )
        age = building_age_years(
            pd.to_numeric(year_built, errors="coerce").to_numpy(dtype="float64"),
            reference_year=self.income_reference_year,
            assumed_age_years=self.assumed_building_age_years,
        )
        effective = effective_operating_expense_ratio(
            age,
            base_ratio=self.operating_expense_ratio,
            per_year=self.maintenance_premium_per_year,
            cap=self.max_maintenance_premium,
        )
        return pd.DataFrame(
            {
                "building_age_years": pd.Series(age, index=index, dtype="float64"),
                "maintenance_premium": pd.Series(
                    effective - self.operating_expense_ratio,
                    index=index,
                    dtype="float64",
                ),
                "effective_operating_expense_ratio": pd.Series(
                    effective, index=index, dtype="float64"
                ),
            },
            index=index,
        )

    def as_metadata(self) -> dict[str, object]:
        """The object every row carries, so a rate can be read back."""
        return {
            "average_rent_cad": _plain(self.average_rent_cad),
            "vacancy_rate_pct": _plain(self.vacancy_rate_pct),
            "operating_expense_ratio": self.operating_expense_ratio,
            # The base above is what a *new* building is charged; these four
            # are the curve that ages it. `urban_rag.hbu` reads the base back
            # out of this object for the building it proposes, which is new by
            # construction - see `operating_expense_ratio_of`.
            "maintenance_premium_per_year": self.maintenance_premium_per_year,
            "max_maintenance_premium": self.max_maintenance_premium,
            "assumed_building_age_years": self.assumed_building_age_years,
            "income_reference_year": _plain(self.income_reference_year),
            "retail_rent_per_sqft_cad": self.retail_rent_per_sqft_cad,
            "office_rent_per_sqft_cad": self.office_rent_per_sqft_cad,
            "industrial_rent_per_sqft_cad": self.industrial_rent_per_sqft_cad,
            "retail_vacancy_pct": self.retail_vacancy_pct,
            "office_vacancy_pct": self.office_vacancy_pct,
            "industrial_vacancy_pct": self.industrial_vacancy_pct,
            "market_value_factor": self.market_value_factor,
            "months_per_year": MONTHS_PER_YEAR,
            "survey_year": _plain(self.survey_year),
            "survey_period": self.survey_period,
            # Which publisher, quarter and submarket each of the three
            # non-residential rates came from, and whether it was measured,
            # escalated or stated. A rate with no provenance beside it cannot
            # be read against next quarter's - the rule `construction_costs`
            # already follows in gold.lot_profiles.
            "rent_provenance": dict(self.rent_provenance),
        }

    def rate_for(self, rent_class: str) -> float | None:
        """The gross annual rent per square foot for one rent class.

        None for `residential` and for `none`: a dwelling is priced per month
        per unit rather than per square foot, and a floor the CUBF could not
        place is not priced at all.
        """
        return {
            "retail": self.retail_rent_per_sqft_cad,
            "office": self.office_rent_per_sqft_cad,
            "industrial": self.industrial_rent_per_sqft_cad,
        }.get(rent_class)

    def vacancy_for(self, rent_class: str) -> float:
        """The stated vacancy netted off one rent class, in percent."""
        return {
            "retail": self.retail_vacancy_pct,
            "office": self.office_vacancy_pct,
            "industrial": self.industrial_vacancy_pct,
        }.get(rent_class, 0.0)


@dataclass(frozen=True)
class ComparableWeights:
    """How much each feature counts, and what one unit of it is.

    Every field is a judgement, and they come in pairs: a *scale* that turns a
    feature's own unit into a dimensionless distance, and a *weight* that says
    how much that distance counts against the others. Separating them is what
    makes either readable - "500 m is one unit of distance" is a claim about
    the borough, and "distance counts as much as floor area" is a claim about
    what makes two lots comparable.

    ``distance_scale_m`` is the ground distance worth one unit. 500 m is a few
    blocks in Villeray, which is about as far as a residential comparable
    usually travels before the street it is on stops being the same street.

    ``size_ratio_scale`` and ``dwellings_ratio_scale`` are the *factor* worth
    one unit. 2.0 means a lot with twice the floor area is one unit away,
    whatever the absolute figures - which is what makes a 90 m2 duplex and a
    180 m2 fourplex as far apart as a 2,000 m2 warehouse and a 4,000 m2 one.

    ``same_class_penalty`` is what a different `rl0105a` inside one CUBF class
    costs: a duplex against a triplex, both Habitation. ``different_class_
    penalty`` is what crossing the class costs, and at 1.0 it is exactly
    `distance_scale_m` of ground - a commercial lot is as unlike a residential
    one as a residential one 500 m away.

    ``missing_penalty`` is what a feature neither side can state costs. Read
    the module docstring for why it is a cost rather than an omission.

    `use_weight` leads the weights at 1.5 because it is the feature a reader
    would refuse to trade away: a comparable of the wrong kind is not a
    comparable at any distance, while one a little larger or a little further
    still is.
    """

    distance_scale_m: float = 500.0
    size_ratio_scale: float = 2.0
    dwellings_ratio_scale: float = 2.0
    same_class_penalty: float = 0.35
    different_class_penalty: float = 1.0
    missing_penalty: float = 1.0

    distance_weight: float = 1.0
    lot_area_weight: float = 1.0
    floor_area_weight: float = 1.0
    dwellings_weight: float = 1.0
    use_weight: float = 1.5

    #: Names of the five weight fields, in the order the composite sums them.
    #: A tuple rather than a dict comprehension over `__dataclass_fields__` so
    #: the payload's key order is this module's decision and not the
    #: dataclass's.
    WEIGHT_FIELDS = (
        ("distance", "distance_weight"),
        ("lot_area", "lot_area_weight"),
        ("floor_area", "floor_area_weight"),
        ("dwellings", "dwellings_weight"),
        ("use_code", "use_weight"),
    )

    def __post_init__(self) -> None:
        for name in ("distance_scale_m", "size_ratio_scale", "dwellings_ratio_scale"):
            value = getattr(self, name)
            if value <= 0:
                raise ValueError(f"{name} must be positive, got {value!r}")
        for name in ("size_ratio_scale", "dwellings_ratio_scale"):
            if getattr(self, name) == 1.0:
                # log(1) is 0 and the component would divide by it. A ratio
                # scale of 1 also means "any difference in size at all is
                # infinitely far", which is not a setting anybody wants.
                raise ValueError(f"{name} must not be 1.0 - log(1.0) is zero")
        for _, field_name in self.WEIGHT_FIELDS:
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name} must not be negative")
        if not any(getattr(self, name) > 0 for _, name in self.WEIGHT_FIELDS):
            # Every distance would be 0 and the "nearest" k lots would be
            # whichever the sort happened to leave first, which is a neighbour
            # list that looks computed and is not.
            raise ValueError("at least one weight must be positive")

    def as_metadata(self) -> dict[str, object]:
        """The object every row carries beside its neighbour list."""
        return {
            "scales": {
                "distance_m": self.distance_scale_m,
                "size_ratio": self.size_ratio_scale,
                "dwellings_ratio": self.dwellings_ratio_scale,
            },
            "penalties": {
                "same_class": self.same_class_penalty,
                "different_class": self.different_class_penalty,
                "missing": self.missing_penalty,
            },
            "weights": {
                name: getattr(self, field_name)
                for name, field_name in self.WEIGHT_FIELDS
            },
        }


# --------------------------------------------------------------------------
# the roll, at lot grain
# --------------------------------------------------------------------------


def aggregate_units_by_lot(
    pairs: pd.DataFrame, units: pd.DataFrame, *, lot_column: str, join_key: str
) -> pd.DataFrame:
    """What the roll says stands on each lot, summed over the units on it.

    ``pairs`` is one row per (unit, lot) - `urban_rag.role_assets` places them,
    by the roll's own cadastre crosswalk and then by where the unit's point
    falls - and ``units`` is one row per unit with the MAMH characteristic
    columns `urban_rag.role_foncier` names. Returns one row per lot, indexed by
    ``lot_column``.

    **This is the aggregation `lot_assessed_values` does not do.** That asset
    sums one column, `rl0404a`, because a total is what it is for. A cap rate
    needs the dwellings that earn the income and the floor that houses it, and
    a comparable needs to know what kind of thing the lot is - none of which
    survives a sum over values.

    **The value columns are deliberately absent.** `total_assessed_value` is
    `lot_assessed_values`' answer and is joined in beside this rather than
    recomputed, so the two tables cannot end up disagreeing about what a lot is
    worth. What is computed here is only what that asset does not carry.

    **Whole, not apportioned.** A unit on three lots contributes its dwellings
    and its floor to each of them, exactly as it contributes its whole value to
    each in `total_assessed_value`. Read the module docstring for why that is
    the safe basis for a *ratio* even though it is the wrong one for a sum.

    **Floor area is split by each unit's own use code**, so a lot whose two
    units are a triplex and a depanneur reports both a residential and a
    commercial floor area. A class the code cannot be placed in - a blank
    `rl0105a`, a 9000-series code - lands in none of the three, which is why
    they need not add up to `floor_area_m2` and why the gap is worth reporting.

    **"Dominant" means the largest share of the value**, not the largest floor
    or the commonest code: the unit carrying most of what the lot is worth is
    the one whose use a reader means when they ask what the lot *is*.
    `year_built` and `num_storeys` are read off that same unit, so the three
    describe one property rather than three different ones.
    """
    if pairs.empty:
        return _empty_aggregate(lot_column)

    # Every characteristic comes from ``units`` and none from ``pairs``, even
    # where the pair frame happens to carry one: the characteristics table is
    # the row that describes the property, and a pair frame is a placement.
    # Taking `rl0404a` from whichever side had it would make the dominant unit
    # depend on which route placed it.
    carried = [column for column in _CARRIED_COLUMNS if column in units.columns]
    joined = pairs[[join_key, lot_column]].merge(
        units[[join_key, *carried]], on=join_key, how="left"
    )
    if joined.empty:
        return _empty_aggregate(lot_column)

    # Coerced once, here: the GeoPackage types several of these as text -
    # `rl0307a` is four characters, not an integer - and a sum over strings
    # either concatenates them or raises, neither of which is a floor area.
    for column in _NUMERIC_CARRIED:
        if column in joined.columns:
            joined[column] = _numeric(joined, column)

    # Two classifications of the same unit, and they are not redundant: the
    # first is what the lot's floor is *reported* as and the second what it is
    # *charged*. They differ only on commerce, which splits into retail and
    # office because the rent surveys do - see `rent_class_of`.
    has_code = _USE_CODE in joined.columns
    joined["income_class"] = (
        joined[_USE_CODE].map(income_class_of) if has_code else UNKNOWN_INCOME_CLASS
    )
    joined["rent_class"] = (
        joined[_USE_CODE].map(rent_class_of) if has_code else UNKNOWN_INCOME_CLASS
    )
    floor = _numeric(joined, _FLOOR_AREA)
    for name in INCOME_CLASSES:
        joined[f"{name}_floor_area_m2"] = floor.where(joined["income_class"] == name)
    for name in ("retail", "office"):
        joined[f"{name}_floor_area_m2"] = floor.where(joined["rent_class"] == name)

    grouped = joined.groupby(lot_column, sort=False)
    # `min_count=1` on every sum, which is the whole reason this is not one
    # `agg` call: a lot whose units state no floor area at all must report
    # null and not 0.0. The comparables charge `missing_penalty` for the first
    # and match it against every empty warehouse in the borough for the
    # second, and the roll leaves the column blank often enough for the
    # difference to matter.
    present = {
        source: target
        for source, target in _SUMMED_COLUMNS.items()
        if source in joined.columns
    }
    summed = grouped[list(present)].sum(min_count=1).rename(columns=present)
    summed.insert(0, "num_assessment_units", grouped[join_key].nunique())
    return summed.join(_dominant_unit(joined, lot_column=lot_column))


def _dominant_unit(joined: pd.DataFrame, *, lot_column: str) -> pd.DataFrame:
    """The use code, year and storeys of each lot's most valuable unit.

    Sorted and then de-duplicated rather than picked with an `idxmax`, which
    raises on a lot whose every unit has a null value - a lane the roll reached
    and assessed at nothing. `na_position="last"` puts those rows behind the
    valued ones, so a lot with one valued unit and three blanks reports the
    valued one, and a lot with only blanks reports its first rather than
    raising.

    ``kind="stable"`` so a tie between two equally valuable units is broken by
    the order they were placed in, which is the crosswalk's order and is the
    same on a re-run. A tie broken differently each run would move
    `dominant_use_code` on a lot nothing about had changed.
    """
    columns = [
        name
        for name in (_USE_CODE, _YEAR_BUILT, _STOREYS)
        if name in joined.columns
    ]
    if not columns:
        return pd.DataFrame(index=pd.Index([], name=lot_column))

    order = [lot_column]
    ascending = [True]
    if _VALUE in joined.columns:
        order.append(_VALUE)
        ascending.append(False)
    ranked = joined.sort_values(
        order, ascending=ascending, na_position="last", kind="stable"
    ).drop_duplicates(subset=[lot_column], keep="first")

    dominant = ranked.set_index(lot_column)[columns].rename(
        columns={
            _USE_CODE: "dominant_use_code",
            _YEAR_BUILT: "year_built",
            _STOREYS: "num_storeys",
        }
    )
    if "dominant_use_code" in dominant.columns:
        dominant["dominant_use_code"] = dominant["dominant_use_code"].map(
            _use_code_text
        )
        dominant["dominant_use_class"] = dominant["dominant_use_code"].map(
            use_class_of
        )
        dominant["dominant_income_class"] = dominant["dominant_use_code"].map(
            income_class_of
        )
    return dominant


def _use_code_text(value) -> str | None:
    """`rl0105a` as the four-character string the roll prints it as.

    The GeoPackage types this column as text, but a frame that has been through
    a parquet round trip with a null in it can hand back a float - and
    ``str(1000.0)`` is ``'1000.0'``, which matches no code, belongs to no class
    and would silently make every such lot a comparable of every other. Coerced
    here, once, on the way into the only column any reader sees it through.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text.zfill(4) if text.isdigit() and len(text) < 4 else text


def _empty_aggregate(lot_column: str) -> pd.DataFrame:
    """The shape above, with no rows, for a partition that placed no unit."""
    numeric = (
        "num_assessment_units",
        "num_dwellings",
        "num_nonresidential_units",
        "num_rental_rooms",
        "floor_area_m2",
        "roll_land_area_m2",
        *(f"{name}_floor_area_m2" for name in INCOME_CLASSES),
        "retail_floor_area_m2",
        "office_floor_area_m2",
        "year_built",
        "num_storeys",
    )
    frame = pd.DataFrame(
        {name: pd.Series(dtype="float64") for name in numeric}
        | {
            name: pd.Series(dtype="object")
            for name in (
                "dominant_use_code",
                "dominant_use_class",
                "dominant_income_class",
            )
        }
    )
    frame.index.name = lot_column
    return frame


# --------------------------------------------------------------------------
# the income side
# --------------------------------------------------------------------------


def annual_income(
    frame: pd.DataFrame, assumptions: IncomeAssumptions
) -> pd.DataFrame:
    """Gross and net annual income for every lot, by class and in total.

    ``frame`` is one row per lot carrying `num_dwellings` and the three
    `*_floor_area_m2` columns `aggregate_units_by_lot` produces. Returns four
    income columns plus the NOI, indexed like ``frame``.

    **The classes are added, not chosen between.** A lot carrying a triplex
    over a depanneur earns both, because its two assessment units say so -
    which is the whole reason the floor was split by the unit's own use code
    one step earlier rather than by a dominant use here.

    **Priced at four rates and reported at three.** Retail and office are
    charged separately, because `silver/commercial_rents` surveys them
    separately and about $4 a square foot separates them in Montreal, and then
    added back into `commercial_income_cad` so the reported columns keep the
    shape every reader downstream already has. `retail_income_cad` and
    `office_income_cad` travel beside it for anyone who wants the split.

    **Residential is per dwelling per month; the rest are per square foot per
    year.** That is how each is quoted, and `MONTHS_PER_YEAR` and `M2_PER_SQFT`
    are the only two conversions between them. Mixing them up is the failure
    this function is most exposed to - a monthly rent read as annual
    understates a building twelve-fold, and a square metre priced as a square
    foot overstates one ten-fold, and both look plausible.

    **Vacancy is netted per class and the expense ratio once at the end.** They
    are different things: vacancy is income never collected, the expense ratio
    is collected income that leaves again, and applying either to the other's
    result would be charging one of them twice.

    **Null, not zero, where a class was not priced.** A borough CMHC published
    no rent for has a null residential income however many dwellings stand in
    it, and a lot whose units carry no floor area has a null commercial one.
    The total is the sum of the classes that *were* priced, and is itself null
    only when none of them was - which is the difference between a lot that
    earns nothing and a lot nobody could price.
    """
    dwellings = _numeric(frame, "num_dwellings")
    residential = pd.Series(np.nan, index=frame.index, dtype="float64")
    if assumptions.has_residential_rent:
        residential = (
            dwellings
            * float(assumptions.average_rent_cad)
            * MONTHS_PER_YEAR
            * assumptions.residential_occupancy
        )

    priced = {
        name: _non_residential_income(
            _numeric(frame, f"{name}_floor_area_m2"),
            rate_per_sqft=assumptions.rate_for(name),
            vacancy_pct=assumptions.vacancy_for(name),
        )
        for name in NON_RESIDENTIAL_RENT_CLASSES
    }
    # Retail and office put back together under the name the reported columns
    # use. `min_count=1` again: a lot with retail floor and no office floor
    # earns its retail income rather than a NaN, and one with neither earns a
    # NaN rather than a zero.
    commercial = (
        pd.concat([priced["retail"], priced["office"]], axis=1)
        .sum(axis=1, min_count=1)
    )

    parts = pd.concat([residential, commercial, priced["industrial"]], axis=1)
    # The same rule at the top level: a row where every class is NaN sums to
    # NaN rather than to 0.0, which is what keeps "nobody could price this" out
    # of "this earns nothing".
    gross = parts.sum(axis=1, min_count=1)
    # The expense ratio is per lot, not per partition: only maintenance among
    # the four things it covers depends on the building's age, and that part is
    # `assumptions.expense_ratios`. A frame with no `year_built` - a partition
    # the roll never reached, or a hand-built test frame - charges the assumed
    # age, which is what that column being absent and being null both mean.
    year_built = (
        frame["year_built"]
        if "year_built" in frame.columns
        else pd.Series(np.nan, index=frame.index, dtype="float64")
    )
    ratios = assumptions.expense_ratios(year_built)
    return pd.DataFrame(
        {
            "residential_income_cad": residential,
            "retail_income_cad": priced["retail"],
            "office_income_cad": priced["office"],
            "commercial_income_cad": commercial,
            "industrial_income_cad": priced["industrial"],
            "gross_income_cad": gross,
            "building_age_years": ratios["building_age_years"],
            "maintenance_premium": ratios["maintenance_premium"],
            "effective_operating_expense_ratio": ratios[
                "effective_operating_expense_ratio"
            ],
            "net_operating_income_cad": gross
            * (1.0 - ratios["effective_operating_expense_ratio"]),
        },
        index=frame.index,
    )


def cap_rate_pct(
    net_operating_income: pd.Series,
    value: pd.Series,
    *,
    market_value_factor: float = 1.0,
) -> pd.Series:
    """NOI over value, in percent, or null where either side is not there.

    Percent rather than a fraction, matching how a cap rate is quoted and how
    `vacancy_rate_pct` beside it is stored - a reader with both in front of
    them should not have to remember that one is scaled and the other is not.

    A value of zero yields null rather than an infinity: a lot the roll reached
    and assessed at nothing is a data problem, not a parcel of unbounded yield,
    and an infinity in this column would poison every average taken over it.
    """
    scaled = pd.to_numeric(value, errors="coerce").astype("float64")
    scaled = scaled * float(market_value_factor)
    denominator = scaled.where(scaled.abs() > _MIN_DENOMINATOR)
    noi = pd.to_numeric(net_operating_income, errors="coerce").astype("float64")
    return 100.0 * noi / denominator


def _non_residential_income(
    area_m2: pd.Series, *, rate_per_sqft: float | None, vacancy_pct: float
) -> pd.Series:
    """Annual income from a class of floor, at its rent and stated vacancy.

    A null rate is a class nothing could price - `rate_for` answers None for a
    class that has no surveyed rent - and returns nulls rather than zeros, the
    same distinction a suppressed CMHC rent draws on the residential side.
    """
    if rate_per_sqft is None:
        return pd.Series(np.nan, index=area_m2.index, dtype="float64")
    return area_m2 / M2_PER_SQFT * float(rate_per_sqft) * _occupancy(vacancy_pct)


# --------------------------------------------------------------------------
# the comparables
# --------------------------------------------------------------------------


def nearest_comparables(
    lots: pd.DataFrame,
    *,
    k: int = 8,
    weights: ComparableWeights | None = None,
    max_distance_m: float = 2000.0,
) -> list[list[dict[str, object]]]:
    """The ``k`` most similar valued lots for each row of ``lots``.

    ``lots`` is one row per lot and must carry the six columns the metric is
    computed over - ``lot_number``, ``x_m`` and ``y_m`` (projected metres),
    ``lot_area_m2``, ``floor_area_m2``, ``num_dwellings`` and ``use_code`` -
    plus ``total_assessed_value``, which decides which rows may be neighbours.

    Returns one list per row, in ``lots``' own order, each entry a plain dict
    naming the neighbour and the four ratios it contributes. A lot with nothing
    inside ``max_distance_m`` gets an empty list, and so does every row when the
    partition holds no valued lot at all.

    **A lot is never its own comparable**, and the exclusion is by position
    rather than by lot number: two rows sharing a number would be a broken
    upstream, and this function should not quietly paper over one by deciding
    which of them is the real lot.
    """
    weights = weights or ComparableWeights()
    if k < 1:
        raise ValueError(f"k must be at least 1, got {k!r}")

    features = _feature_arrays(lots)
    valued = np.asarray(
        pd.to_numeric(lots.get("total_assessed_value"), errors="coerce").to_numpy(
            dtype="float64", na_value=np.nan
        )
    )
    candidates = np.flatnonzero(np.isfinite(valued))
    if candidates.size == 0:
        return [[] for _ in range(len(lots))]

    payloads = _candidate_payloads(lots, candidates)
    results: list[list[dict[str, object]]] = []
    for start in range(0, len(lots), _CHUNK_ROWS):
        stop = min(start + _CHUNK_ROWS, len(lots))
        distance, ground = _composite_distance(
            features, candidates, start, stop, weights
        )
        # A lot cannot be its own comparable. Masked after the metric rather
        # than by dropping the row, so the candidate indices stay aligned with
        # `payloads` for every chunk.
        rows = np.arange(start, stop)[:, None]
        distance = np.where(candidates[None, :] == rows, np.inf, distance)
        distance = np.where(ground > max_distance_m, np.inf, distance)
        results.extend(
            _take_k(distance, ground, payloads, k=k, weights=weights)
        )
    return results


def _feature_arrays(lots: pd.DataFrame) -> dict[str, np.ndarray]:
    """``lots`` as the six aligned arrays the metric is computed over.

    Logged here rather than inside the chunk loop: `log1p` over a borough is
    one pass, and doing it per chunk would repeat it once per 512 subjects.
    `log1p` and not `log` because a lot with no floor and no dwellings is a
    real row and log(0) is not a number the metric can carry - the +1 makes
    "none" the origin of the size axes rather than a negative infinity on them.
    """
    return {
        "x": _values(lots, "x_m"),
        "y": _values(lots, "y_m"),
        "lot_area": np.log1p(np.maximum(_values(lots, "lot_area_m2"), 0.0)),
        "floor_area": np.log1p(np.maximum(_values(lots, "floor_area_m2"), 0.0)),
        "dwellings": np.log1p(np.maximum(_values(lots, "num_dwellings"), 0.0)),
        # Codes as small integers so the comparison below is one numpy op on
        # ints rather than a python-level string compare per pair. -1 is
        # "unstated", and never equal to itself: `_use_distance` tests for it
        # explicitly rather than letting two unknowns look identical.
        "use_code": _use_codes(lots),
        "use_class": _use_classes(lots),
    }


def _use_codes(lots: pd.DataFrame) -> np.ndarray:
    """`rl0105a` as an integer per row, or -1 where it is not four digits."""
    raw = lots.get("use_code")
    if raw is None:
        return np.full(len(lots), -1, dtype="int64")
    codes = raw.map(
        lambda value: int(str(value).strip())
        if _cubf_entry(value) is not None
        else -1
    )
    return codes.to_numpy(dtype="int64")


def _use_classes(lots: pd.DataFrame) -> np.ndarray:
    """The CUBF class as its leading digit, or -1 where there is none."""
    raw = lots.get("use_code")
    if raw is None:
        return np.full(len(lots), -1, dtype="int64")
    classes = raw.map(
        lambda value: int(str(value).strip()[0])
        if _cubf_entry(value) is not None
        else -1
    )
    return classes.to_numpy(dtype="int64")


def _composite_distance(
    features: dict[str, np.ndarray],
    candidates: np.ndarray,
    start: int,
    stop: int,
    weights: ComparableWeights,
) -> tuple[np.ndarray, np.ndarray]:
    """The weighted norm for one chunk, and the ground distance behind it.

    Both are returned because the second is wanted twice: it bounds the answer
    through ``max_distance_m``, and it travels into every neighbour entry in
    metres, which is the only component of the metric a reader can check by
    looking at a map.
    """
    subject = slice(start, stop)
    dx = features["x"][subject][:, None] - features["x"][candidates][None, :]
    dy = features["y"][subject][:, None] - features["y"][candidates][None, :]
    ground = np.hypot(dx, dy)

    total = weights.distance_weight * (ground / weights.distance_scale_m) ** 2
    total = total + weights.lot_area_weight * _ratio_distance(
        features["lot_area"], candidates, subject, weights.size_ratio_scale, weights
    ) ** 2
    total = total + weights.floor_area_weight * _ratio_distance(
        features["floor_area"], candidates, subject, weights.size_ratio_scale, weights
    ) ** 2
    total = total + weights.dwellings_weight * _ratio_distance(
        features["dwellings"],
        candidates,
        subject,
        weights.dwellings_ratio_scale,
        weights,
    ) ** 2
    total = total + weights.use_weight * _use_distance(
        features["use_code"], features["use_class"], candidates, subject, weights
    ) ** 2
    return np.sqrt(total), ground


def _ratio_distance(
    logged: np.ndarray,
    candidates: np.ndarray,
    subject: slice,
    ratio_scale: float,
    weights: ComparableWeights,
) -> np.ndarray:
    """A size ratio as a dimensionless distance, or the missing penalty.

    ``logged`` is already `log1p` of the quantity, so the difference of two
    entries is the log of their ratio and dividing by ``log(ratio_scale)``
    turns "twice as big" into 1.0 whatever the absolute sizes are.
    """
    left = logged[subject][:, None]
    right = logged[candidates][None, :]
    distance = np.abs(left - right) / math.log(ratio_scale)
    # NaN on either side is a quantity the roll did not state for that lot, and
    # `np.abs` of it is NaN - so the penalty lands exactly where a component
    # could not be computed, and nowhere else.
    return np.where(np.isnan(distance), weights.missing_penalty, distance)


def _use_distance(
    codes: np.ndarray,
    classes: np.ndarray,
    candidates: np.ndarray,
    subject: slice,
    weights: ComparableWeights,
) -> np.ndarray:
    """0 for the same use code, then the class penalty, then the missing one.

    Ordered so that the cheapest match wins: a pair sharing a four-digit code
    is never charged the class penalty for also sharing a class, and a pair
    where either side is unstated is charged `missing_penalty` regardless of
    what the other one says - two blanks are not a match.
    """
    left_code = codes[subject][:, None]
    right_code = codes[candidates][None, :]
    left_class = classes[subject][:, None]
    right_class = classes[candidates][None, :]

    known = (left_code >= 0) & (right_code >= 0)
    same_code = known & (left_code == right_code)
    same_class = known & (left_class == right_class)
    return np.where(
        same_code,
        0.0,
        np.where(
            same_class,
            weights.same_class_penalty,
            np.where(known, weights.different_class_penalty, weights.missing_penalty),
        ),
    )


def _candidate_payloads(
    lots: pd.DataFrame, candidates: np.ndarray
) -> list[dict[str, object]]:
    """What a neighbour says about itself, built once for the whole partition.

    One dict per candidate rather than one per (subject, neighbour) pair: a
    borough's 22,000 lots each naming eight neighbours is 176,000 entries, and
    rebuilding the same twenty-odd payloads from a frame that many times is
    where a straightforward implementation of this asset spends its afternoon.
    The per-pair distances are added to a copy at the point of use.
    """
    columns = (
        "lot_number",
        "use_code",
        "lot_area_m2",
        "floor_area_m2",
        "num_dwellings",
        "num_assessment_units",
        "total_assessed_value",
    )
    slim = lots.iloc[candidates]
    frame = pd.DataFrame(
        {name: slim[name] for name in columns if name in slim.columns}
    )
    value = _numeric(frame, "total_assessed_value")
    frame["value_per_dwelling_cad"] = _safe_ratio(
        value, _numeric(frame, "num_dwellings")
    )
    frame["value_per_floor_m2_cad"] = _safe_ratio(
        value, _numeric(frame, "floor_area_m2")
    )
    frame["value_per_land_m2_cad"] = _safe_ratio(
        value, _numeric(frame, "lot_area_m2")
    )
    return _records(frame)


def _take_k(
    distance: np.ndarray,
    ground: np.ndarray,
    payloads: Sequence[dict[str, object]],
    *,
    k: int,
    weights: ComparableWeights,
) -> list[list[dict[str, object]]]:
    """The ``k`` smallest finite distances of each row, nearest first.

    `argpartition` rather than a full `argsort`: only the k smallest are
    wanted, and partitioning a borough-wide row is linear in the candidates
    where sorting it is not. The k that come back are then sorted among
    themselves, which is a sort of eight things.
    """
    width = distance.shape[1]
    take = min(k, width)
    partitioned = np.argpartition(distance, take - 1, axis=1)[:, :take]
    rows: list[list[dict[str, object]]] = []
    for position in range(distance.shape[0]):
        picks = partitioned[position]
        picks = picks[np.isfinite(distance[position, picks])]
        picks = picks[np.argsort(distance[position, picks], kind="stable")]
        rows.append(
            [
                {
                    **payloads[index],
                    "distance": round(float(distance[position, index]), 6),
                    "distance_m": round(float(ground[position, index]), 1),
                }
                for index in picks
            ]
        )
    return rows


def summarise_comparables(
    neighbours: Sequence[Sequence[Mapping[str, object]]],
    *,
    index: pd.Index,
) -> pd.DataFrame:
    """The three median ratios and two counts, one row per subject.

    Median rather than mean, for the reason every appraisal takes medians: one
    condominium tower's common-parts lot carrying 402 units and $258M sits in
    the same neighbour list as four triplexes, and a mean would let it decide
    the answer on its own. The median of an even count is the midpoint of the
    two middle values, which is `numpy.median`'s own definition and is what a
    reader of "the median comparable" expects.

    Each ratio is taken over the neighbours that *have* it, not over all of
    them: a commercial comparable with no dwellings contributes to the floor
    and land medians and is simply absent from the per-dwelling one. That is
    why the three can rest on three different denominators, and why
    `num_comparables` is reported beside them rather than standing in for all
    three.
    """
    records = []
    for entries in neighbours:
        distances = [
            float(entry["distance"])
            for entry in entries
            if entry.get("distance") is not None
        ]
        ground = [
            float(entry["distance_m"])
            for entry in entries
            if entry.get("distance_m") is not None
        ]
        records.append(
            {
                "num_comparables": len(entries),
                "comparable_mean_distance": (
                    round(float(np.mean(distances)), 6) if distances else None
                ),
                "comparable_median_distance_m": (
                    round(float(np.median(ground)), 1) if ground else None
                ),
                **{
                    f"comparable_{name}": _median_of(entries, name)
                    for name in (
                        "value_per_dwelling_cad",
                        "value_per_floor_m2_cad",
                        "value_per_land_m2_cad",
                    )
                },
            }
        )
    return pd.DataFrame(records, index=index)


def estimate_value(frame: pd.DataFrame) -> pd.DataFrame:
    """What the comparables say the lot is worth, and which ratio said it.

    The bases are tried in `VALUE_BASES` order and the first that can answer
    wins - dollars per dwelling where the lot has dwellings and the neighbours
    quoted one, then per square metre of floor, then per square metre of
    ground. `estimated_value_basis` names the winner on every row, because
    three estimates arrived at three ways are not interchangeable and a column
    that did not say which it was would be read as though they were.

    Ground area is last and is what makes the column complete: it is the only
    ratio a parcel carrying nothing at all can be valued on, and valuing the
    vacant lots off the built ones around them is most of the reason this
    asset exists.
    """
    estimate = pd.Series(np.nan, index=frame.index, dtype="float64")
    basis = pd.Series("none", index=frame.index, dtype="object")
    for name, ratio_column, quantity_column in (
        ("per_dwelling", "comparable_value_per_dwelling_cad", "num_dwellings"),
        ("per_floor_area", "comparable_value_per_floor_m2_cad", "floor_area_m2"),
        ("per_land_area", "comparable_value_per_land_m2_cad", "lot_area_m2"),
    ):
        ratio = _numeric(frame, ratio_column)
        quantity = _numeric(frame, quantity_column)
        candidate = ratio * quantity
        # Only where nothing has answered yet, and only where the quantity is
        # a quantity: a lot with zero dwellings must fall through to the floor
        # basis rather than be valued at zero dollars by the first one.
        fill = estimate.isna() & candidate.notna() & (quantity > _MIN_DENOMINATOR)
        estimate = estimate.mask(fill, candidate)
        basis = basis.mask(fill, name)
    return pd.DataFrame(
        {"estimated_value_cad": estimate, "estimated_value_basis": basis}
    )


# --------------------------------------------------------------------------
# small shared pieces
# --------------------------------------------------------------------------


def _median_of(
    entries: Sequence[Mapping[str, object]], name: str
) -> float | None:
    values = [
        float(entry[name])
        for entry in entries
        if entry.get(name) is not None and np.isfinite(float(entry[name]))
    ]
    return round(float(np.median(values)), 4) if values else None


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """``numerator / denominator``, null where the denominator is not there.

    Guarded rather than left to numpy, which answers a division by zero with an
    infinity and a warning. An infinite dollars-per-dwelling would survive into
    a neighbour's payload, then into a median, and then into an estimated value
    that no reader would be able to trace back to the empty lot that produced
    it.
    """
    safe = denominator.where(denominator.abs() > _MIN_DENOMINATOR)
    return numerator / safe


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    """One column as float64 with missing values as NaN, or all-NaN if absent.

    Absent rather than raising, because the columns this reaches for are the
    ones an upstream may legitimately not carry - a partition whose roll has no
    floor area for a single unit still has every other column - and a NaN
    column produces null measures, which is the right answer, where a KeyError
    would produce none at all.
    """
    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce").astype("float64")


def _values(frame: pd.DataFrame, column: str) -> np.ndarray:
    return _numeric(frame, column).to_numpy(dtype="float64", na_value=np.nan)


def _occupancy(vacancy_pct: float | None) -> float:
    """``1 - vacancy``, from a rate published in percent.

    A missing rate is full occupancy rather than none: the rent beside it was
    measured, and inventing a vacancy for a cell CMHC suppressed would move the
    answer further from the truth than assuming the units are let. A published
    rate outside [0, 100] is clamped rather than trusted - a negative occupancy
    would turn income into a loss and a cap rate's sign is not something a
    typo in a survey should decide.
    """
    if vacancy_pct is None:
        return 1.0
    try:
        rate = float(vacancy_pct)
    except (TypeError, ValueError):
        return 1.0
    if math.isnan(rate):
        return 1.0
    return 1.0 - min(max(rate, 0.0), 100.0) / 100.0


def _plain(value):
    """A numpy or pandas scalar as something `json.dumps` will take."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except (AttributeError, ValueError):
            return value
    return value


def _records(frame: pd.DataFrame) -> list[dict[str, object]]:
    """``frame`` as plain JSON-able dicts, with missing values as null.

    The same round trip `urban_rag.lot_profiles_assets._records` makes, and for
    the same reason: `to_dict` hands back `nan`, `NaT` and numpy scalars, none
    of which psycopg adapts into jsonb and each of which lands as the string
    "NaN" if it reaches `json.dumps`.
    """
    if frame.empty:
        return []
    import json

    return json.loads(frame.to_json(orient="records", date_format="iso"))
