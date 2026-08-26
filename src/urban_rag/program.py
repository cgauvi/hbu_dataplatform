"""The most rent a lot's zoning envelope can carry, solved with CP-SAT.

A highest-and-best-use question ends in a number: given what the *grille des
usages et des normes* lets you build on this parcel, and what CMHC says the
neighbourhood pays, what unit mix is worth the most? Everything upstream of
this module - the cadastre, the spatial joins, the frontage, the rent and
vacancy grids - exists to fill in the inputs of that one question.

The answer is an integer program, and deliberately not a formula. Three of the
four caps interact: the dwelling ceiling is a count, the density cap is a floor
*area*, and the site-coverage cap is a footprint that only becomes floor area
once multiplied by a number of storeys that is itself capped. `gross floor
area = footprint x floors` is a product of two decision variables, which is
what rules out a linear program and what `AddMultiplicationEquality` is for.
Add the requirement that dwellings come in whole numbers and CP-SAT is the tool
that fits the shape of the problem rather than the one that happens to be
installed.

**What this module is handed, and what it is not.** It takes a `ZoneColumn`
already parsed out of a zoning grid, not a PDF. `urban_rag.zoning_grid` is what
produces one, and it is a separate module for a reason: `linked_documents`
flattens a grid to text for the embedding corpus and stops there, and the
flattened text does not survive the trip - `pypdf`'s default extraction drops
the column alignment that says *which* column an `X` belongs to, so
`Tous sauf le RDC   X` cannot be attributed to the Habitation column without
the x-coordinates that `extraction_mode="layout"` keeps. Keeping the two apart
is also what makes this module testable: the arithmetic below is exercised
against hand-written envelopes rather than against heterogeneous municipal
PDFs. `lot_zoning_envelopes` is where a parsed column meets a real parcel - one
row of it is one call to `solve_program`.

**Units.** The zoning grid is metric and the unit schedule is imperial, so the
two are converted at exactly one place (`M2_PER_SQFT`) and every constraint is
built in square metres. Mixing them is the failure this module is most exposed
to: a density ratio computed with square feet over square metres is wrong by a
factor of ten and looks entirely plausible.

**Vacancy is a percentage.** `vacancy_rates` stores rates as published - 0.2%
is `0.2`, not `0.002` - so the occupancy factor divides by 100. Reading that
column as a fraction understates revenue by two orders of magnitude, which is
the kind of error that produces a confident answer rather than an exception.

**Parking is where the envelope stops being one number.** Every dwelling owes
stalls (`ParkingRules.stalls_per_dwelling`) and so does every thousand square
feet of commerce or industry (`ParkingRules.stalls_per_1000_sqft`), and the two
ways to provide them are not substitutes. Article 38 1° of by-law 01-283
excludes *une aire de stationnement des véhicules [...] située en sous-sol, de
même que leurs voies
d'accès* from the *superficie de plancher* the density index is computed on -
so an underground stall is invisible to the *Densité* cap and, being below
grade, to *En étage* as well. An above-grade stall is neither: it sits in a
storey that counts against both. The trade the solver is asked to make is
therefore a real one - underground buys FSR back at a higher price per stall,
above grade is cheaper per stall but is paid for in dwellings that no longer
fit. Neither dominates, which is why both are decision variables.

**One footprint, identical floors.** Every storey - residential, above-grade
parking, underground - is the same plate, so the footprint is whatever the
hungriest floor type needs and the model carries a single `footprint`
variable. That is an assumption about the building, not a norm: a real garage
would run wider than the tower above it (article 43 excludes *une partie du
bâtiment qui est entièrement sous terre* from the site-coverage calculation,
so it could), and modelling that would need a second footprint.

**The objective is net operating income.** Revenue is `rent x (1 - vacancy)`
a month, and against it stand the two things the building costs to put up:
the dwellings at `ConstructionCosts.residential_cost_per_sqft` over the
schedule in `UNIT_AREAS_SQFT`, and the stalls at `ParkingRules`' per-stall
prices. Both are capital and the revenue is monthly, so both are converted at
`AMORTIZATION_MONTHS` - straight line, undiscounted, no financing. That
horizon is a *stated assumption* rather than a measurement, and it is the
first number to change when the answer looks wrong.

What NOI here does **not** net out is everything nobody handed this module:
land, soft costs, municipal taxes, insurance, management, maintenance, and
any revenue the stalls themselves might earn. It is income less the cost of
building the thing that earns it, and no further.

**Why the revenue is not scaled by area.** An earlier objective maximised
`area_sqft x rent x (1 - vacancy)`, which is dollars-times-square-feet and
was only ever a proxy for "bigger is better". Netting a real cost against it
would not work: a three-bedroom would show $2 029 800 of monthly "revenue"
against $1 220 of monthly construction, and the cost side would be rounding
error on a number that was never dollars. CMHC publishes rent per dwelling
per month, so that is what the revenue is, and the unit schedule earns its
keep on the cost side instead - where a square foot genuinely is priced.

The mix this produces is different, and the difference is the point: rent per
square foot falls as dwellings get larger while construction cost per square
foot does not, so a binding envelope now fills with the class that earns most
per square metre rather than the class that is simply biggest.

**Dwellings are not the only thing a column authorises.** A column headed
``H.2, C.2`` permits commerce beside the housing and one headed ``I.1``
permits industry instead of it, so the envelope those columns describe can be
filled with more than one kind of space. `commercial_floors` and
`industrial_floors` are storeys of that space - whole plates, like the parking
storeys and for the same "one footprint, identical floors" reason - and they
compete with the dwellings for the storeys *En etage* allows and for the floor
area *Densite* allows. They exist only where the column's own usage codes
authorise them, so a pure ``H`` column solves exactly the problem it solved
before.

Non-residential space is priced and rented **per square foot**, which is the
one place its arithmetic differs from a dwelling's: CMHC surveys a rent per
dwelling because a two-bedroom is a two-bedroom, and nobody leases "a retail
unit". Costs are `COMMERCIAL_COST_PER_SQFT_CAD` and
`INDUSTRIAL_COST_PER_SQFT_CAD`, amortised over the same `AMORTIZATION_MONTHS`
as everything else. Revenues are `COMMERCIAL_REVENUE_PER_SQFT_CAD` and
`INDUSTRIAL_REVENUE_PER_SQFT_CAD` less `COMMERCIAL_VACANCY_PCT` and
`INDUSTRIAL_VACANCY_PCT` - the same `rent x (1 - vacancy)` a dwelling gets,
with the vacancy stated rather than surveyed. The rents are **annual**, the
unit commercial leasing is quoted in, divided by `MONTHS_PER_YEAR` to sit
beside a monthly dwelling rent. Reading $80/sf as a monthly figure would let a
retail floor collect $960 a square foot a year, which is the same
order-of-magnitude error the vacancy note above is about, pointed the other
way.

**And that space is what makes the parking bite.** A dwelling owes half a
stall; a thousand square feet of commerce owes three, and a stall is 300
square feet of the plate it sits on. A retail storey therefore very nearly
demands a parking storey of its own - floor area *Densite* counts and a storey
*En etage* counts - which is what stops commerce from simply taking every
storey a mixed-use column allows. Whether it still wins is now a question
about the parcel rather than a foregone conclusion, and that is the point of
putting the ratio in.

`NO_PARKING` and `NO_CONSTRUCTION_COST` strip either cost back out, which is
what the tests that pin the rent arithmetic use so a change to a published
rate cannot move a number that is about something else. `NO_NON_RESIDENTIAL`
does the same for the commerce and the industry, leaving the all-residential
question the module answered before they existed.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum

from ortools.sat.python import cp_model

#: Square metres in a square foot, exactly. The grid's areas, widths and
#: ratios are metric; the unit schedule below is in square feet.
M2_PER_SQFT = 0.09290304

#: Rentable area per dwelling, in square feet, keyed by CMHC's bedroom class.
#:
#: Keyed that way rather than to "studio/1br/2br/3br" so a rent or vacancy
#: frame joins straight on - `cmhc.BEDROOM_TYPES` is where those spellings come
#: from, and CMHC's top class is `3_bedroom_plus` ("3 chambres +") rather than
#: exactly three. A three-bedroom is what it is sized as here.
UNIT_AREAS_SQFT: Mapping[str, float] = {
    "studio": 500.0,
    "1_bedroom": 600.0,
    "2_bedroom": 900.0,
    "3_bedroom_plus": 1200.0,
}

#: Areas are held as integers in hundredths of a square metre. CP-SAT is an
#: integer solver, and rounding a 46.45 m2 studio to a whole 46 would lose ~1%
#: of every unit's area - which compounds across a hundred of them into a
#: floor's worth of slack in the density constraint.
AREA_SCALE = 100

#: The objective is money, and it is an integer for the same reason - held in
#: ten-thousandths of a dollar rather than in cents. Cents are ample for a
#: dwelling, whose coefficient is hundreds of dollars a month, but a square
#: metre of industrial floor nets about twenty cents a month and enters the
#: objective per *hundredth* of a square metre: a fifth of a cent, which
#: rounds to a whole cent with over a percent of error and can tip a choice
#: between two kinds of space that are genuinely close. Four decimal places
#: put that rounding back under a basis point for every term.
MONEY_SCALE = 10_000

#: Commercial and industrial rents are quoted per square foot per *year*; the
#: objective is a month. This is the whole of the conversion between them.
MONTHS_PER_YEAR = 12

#: Stalls owed per dwelling. Villeray's own by-law abolished residential
#: parking minima, so this is a *program* assumption - what the building is
#: being designed to offer - rather than a norm read off the grid.
STALLS_PER_DWELLING = 0.5

#: Stalls owed per thousand square feet of commercial or industrial floor. The
#: same kind of assumption and a much heavier one: at three per thousand a
#: 3 000 sq ft retail plate owes nine stalls, and nine above-grade stalls at
#: `ABOVE_GRADE_STALL_AREA_SQFT` fill 2 700 sq ft - very nearly a second plate
#: of the same size. Commerce that pays for its own parking storey is a
#: different proposition from commerce that does not, and this ratio is the
#: whole of the difference; it is one number precisely so it can be moved.
#:
#: One rate for both, because the demand a use generates is about trips rather
#: than about what the guide charges to build the floor - and because nothing
#: upstream distinguishes a `C.2` warehouse-club from a `C.1` corner store.
STALLS_PER_1000_SQFT = 3.0

#: Stall demand is accumulated at millionths so both ratios stay exact against
#: the things they are charged on. A dwelling ratio would be happy at
#: hundredths, but the non-residential ratio is charged against an *area*
#: held in hundredths of a square metre - about a three-thousandth of a stall
#: apiece - and at hundredths that coefficient would round to nothing at all.
#:
#: The scale is also what keeps a part stall rounding **up**, which is how a
#: ratio is read everywhere one is written into a by-law: the constraint is
#: `SCALE x stalls >= demand`, so 13 dwellings at 0,5 owe 7 stalls, not 6.
STALL_DEMAND_SCALE = 1_000_000

#: Area one stall occupies, in square feet, including its share of the aisles
#: and ramps reaching it. Underground is the larger of the two: the same car
#: needs more structure around it below grade - columns on a grid the tower
#: above dictates rather than the parking layout, plus the ramp that article
#: 38 1° counts as part of the excluded area.
UNDERGROUND_STALL_AREA_SQFT = 400.0
ABOVE_GRADE_STALL_AREA_SQFT = 300.0

#: Dollars per stall, Montreal, from the Altus Group Canadian Cost Guide as
#: `urban_rag.estimator` publishes it: the midpoint of `parkade_ug`'s mtl
#: `[51 925, 68 675]` and `parkade_ag`'s mtl `[38 500, 57 750]`, both flagged
#: `perStall`. Underground is the dearer per stall *and* the larger per stall.
#: Above grade would dominate it outright were the FSR it burns not the more
#: expensive half of the bargain, which is what makes the choice a choice.
UNDERGROUND_STALL_COST_CAD = 60_300.0
ABOVE_GRADE_STALL_COST_CAD = 48_125.0

#: Months a capital cost is spread over to be comparable with a monthly rent.
#: Straight line, undiscounted, 25 years, and shared by the stalls and the
#: dwellings so the two are charged on the same footing. See the module
#: docstring: this is the assumption that lets a one-time cost be subtracted
#: from a recurring revenue at all, and the first number to change if the
#: answer looks wrong.
AMORTIZATION_MONTHS = 300

#: Dollars per square foot of dwelling, Montreal, from the Altus Group
#: Canadian Cost Guide as `urban_rag.estimator` publishes it: the midpoint of
#: `condo_wood`'s mtl `[225, 290]` - "Wood Frame Condo (Up to 6 Storeys)",
#: which is `estimator.LOW_RISE_CONDO_TYPE_ID` and for the same reason. A
#: borough zoned two-to-six storeys builds wood frame, and the band a lot ends
#: up in is the solver's *answer* rather than its input, so this cannot be
#: picked off the storey count without branching on a decision variable.
#:
#: The guide prices four bands above it - `condo_12` at mtl `[275, 335]` is
#: the next - and a lot whose grid allows a tower is being under-costed here.
#: Pass `ConstructionCosts(residential_cost_per_sqft=...)` where the structure
#: is known; `estimator.CONDO_TYPE_IDS` is the list of bands to read it from.
RESIDENTIAL_COST_PER_SQFT_CAD = 257.5

#: Dollars per square foot of commercial and of industrial floor, Montreal,
#: rounded from the `montreal_nonresidential_costs` snapshot: a round average
#: across the guide's `cat="commercial"` rows - offices by storey band and
#: class, fitouts, retail, hotels - and across its `cat="industrial"` rows -
#: warehouse, distribution, urban storage. Averaged rather than picked, and
#: rounded rather than carried to the cent, because neither the grid nor
#: anything else upstream says *which* commerce a `C.2` column will be filled
#: with, and a spuriously precise rate would suggest it did. The snapshot is
#: where the rows behind these live - `estimator.NON_RESIDENTIAL_CATEGORIES`
#: is the list of `cat` values it keeps - and
#: `ConstructionCosts(commercial_cost_per_sqft=...)` is how to pass a
#: particular one where the tenant is known.
#:
#: Unlike `RESIDENTIAL_COST_PER_SQFT_CAD` these are charged against *gross*
#: floor area rather than against a schedule of leasable units, so the
#: corridors and cores the residential figure quietly leaves unpriced are paid
#: for here. The two sides of the ledger are consistent about it: the revenue
#: below is charged on the same gross area.
COMMERCIAL_COST_PER_SQFT_CAD = 300.0
INDUSTRIAL_COST_PER_SQFT_CAD = 200.0

#: Dollars of rent per square foot per **year**, gross. A stated program
#: assumption, not a published survey - there is no CMHC for retail, and the
#: guide this module's costs come from prices building and not leasing. The
#: unit is the one commercial leasing is quoted in, and `MONTHS_PER_YEAR` is
#: where it meets a monthly dwelling rent.
#:
#: Asking rent rather than effective: `COMMERCIAL_VACANCY_PCT` below is what
#: turns one into the other, the same way CMHC's published vacancy does for a
#: dwelling.
COMMERCIAL_REVENUE_PER_SQFT_CAD = 80.0
INDUSTRIAL_REVENUE_PER_SQFT_CAD = 30.0

#: Share of the floor that earns nothing, in percent, as published vacancy is
#: written. Stated rather than surveyed - there is no CMHC for retail, and the
#: cost guide upstream prices building and not leasing - which is the one way
#: these differ from the rates in `UnitEconomics.vacancy_rate_pct` beside them.
#: Read as a percentage for exactly that reason: a reader who has both objects
#: in front of them should not have to remember that one is a fraction.
#:
#: The same figure for both because it is one assumption, not two measurements.
#: A market where retail is soft and industrial is not is modelled by moving
#: them apart, which is what the two fields on `NonResidentialEconomics` are.
COMMERCIAL_VACANCY_PCT = 7.0
INDUSTRIAL_VACANCY_PCT = 7.0

#: Underground levels the model may stack. Not a norm - nothing in the grid
#: bounds excavation - but a domain has to end somewhere, and a solution
#: sitting on this one is reported as `max_underground_levels` in `binding`
#: rather than passed off as the envelope's own answer.
MAX_UNDERGROUND_LEVELS = 6

#: A usage code that authorises dwellings: bare ``H`` as printed in zone
#: C01-001, or one of the numbered classes ``H.1``..``H.7``, optionally
#: lettered (``H.7A``). Footnote markers - CMHC-style ``C.3(9)`` - are stripped
#: before matching, since a footnote qualifies a usage rather than renaming it.
_RESIDENTIAL_USAGE = re.compile(r"^H(?:\.\d+[A-Za-z]?)?$")

#: The same shape for the other two families of usage the model can now fill an
#: envelope with: ``C``/``C.1``..``C.7`` for *Commerce* and ``I``/``I.1``..
#: ``I.4`` for *Industrie*. *Equipements collectifs* (``E``) is authorised by
#: plenty of columns and is deliberately absent - a school or a clinic is not
#: something a proforma rents by the square foot, and pricing one as if it were
#: would put a number on a decision nobody in this pipeline is making.
_COMMERCIAL_USAGE = re.compile(r"^C(?:\.\d+[A-Za-z]?)?$")
_INDUSTRIAL_USAGE = re.compile(r"^I(?:\.\d+[A-Za-z]?)?$")
_FOOTNOTE = re.compile(r"\(\s*\d+\s*\)")


class ProgramError(ValueError):
    """An envelope or a price list that cannot be turned into a model."""


class BuildingLevel(str, Enum):
    """The rows of the grid's *Niveaux de bâtiment autorisés* block.

    A grid marks the levels a usage may occupy with an `X` in that usage's
    column, and more than one row can be marked - a column carrying both
    *Rez-de-chaussée* and *Tous sauf le RDC* is authorised everywhere, spelled
    as two rules instead of one. `permitted_floors` is where they add up.
    """

    GROUND = "rez_de_chaussee"
    BELOW_GROUND = "inferieurs_au_rdc"
    SECOND = "immediatement_superieur_au_rdc"
    ALL_EXCEPT_GROUND = "tous_sauf_le_rdc"
    ALL = "tous_les_niveaux"

    def __str__(self) -> str:
        return self.value


def is_residential_usage(usage: str) -> bool:
    """Whether ``usage`` is one of the Habitation classes.

    Matches the whole code, so ``C.4`` and ``I.2`` are rejected and the ``H``
    inside a word is not mistaken for one.
    """
    return bool(_RESIDENTIAL_USAGE.match(_normalise_usage(usage)))


def is_commercial_usage(usage: str) -> bool:
    """Whether ``usage`` is one of the Commerce classes.

    Anchored the same way `is_residential_usage` is, which is what keeps
    ``CH.1`` and ``Commerce`` out: the code is the whole string or it is not
    the code.
    """
    return bool(_COMMERCIAL_USAGE.match(_normalise_usage(usage)))


def is_industrial_usage(usage: str) -> bool:
    """Whether ``usage`` is one of the Industrie classes."""
    return bool(_INDUSTRIAL_USAGE.match(_normalise_usage(usage)))


def _normalise_usage(usage: str) -> str:
    """A usage code with its footnote marker and its whitespace taken off."""
    return _FOOTNOTE.sub("", usage).strip()


def permitted_floors(levels: Iterable[BuildingLevel], total_floors: int) -> int:
    """How many storeys a usage may occupy, given the rows marked for it.

    Each marked row contributes the storeys it names, and the contributions add
    up because the grid states them as separate authorisations:

    ===============================  ==========================
    Row                              Storeys
    ===============================  ==========================
    Tous les niveaux                 ``total_floors``
    Tous sauf le RDC                 ``total_floors - 1``
    Rez-de-chaussée (RDC)            1
    Inférieurs au RDC                1
    Immédiatement supérieur au RDC   1
    ===============================  ==========================

    The sum is capped at ``total_floors``: *Rez-de-chaussée* alongside *Tous
    sauf le RDC* covers the building exactly once, not the ground floor twice.
    A cellar counts as a storey here because the model's floors are identical
    by assumption - it is buildable area either way, and the grid's own
    *En étage* maximum is what bounds the total.
    """
    if total_floors < 0:
        raise ProgramError(f"total_floors must not be negative, got {total_floors}")
    contributions = {
        BuildingLevel.ALL: total_floors,
        BuildingLevel.ALL_EXCEPT_GROUND: max(total_floors - 1, 0),
        BuildingLevel.GROUND: 1,
        BuildingLevel.BELOW_GROUND: 1,
        BuildingLevel.SECOND: 1,
    }
    allowed = sum(contributions[level] for level in set(levels))
    return min(allowed, total_floors)


@dataclass(frozen=True)
class ZoneColumn:
    """One column of a *grille des usages et des normes*.

    A grid is read down its columns: each is a separate set of rules for the
    usages listed at its head, and a zone with three of them is three
    envelopes, not one. Every field here is optional except the usages and the
    storey maximum, because a grid prints ``-`` wherever a norm does not apply
    and an absent norm is not a norm of zero - ``Largeur du terrain min (m) -``
    means *any* width qualifies, and reading it as 0 happens to be equivalent
    while reading ``Densité min/max -`` as 0/0 forbids building at all.
    """

    #: The codes at the head of the column, e.g. ``("H.2",)`` or ``("H",)``.
    usages: tuple[str, ...]
    #: *En étage min/max* - the building's storey ceiling, before the level
    #: rows narrow it to what this usage may occupy.
    floors_max: int
    #: The rows of *Niveaux de bâtiment autorisés* marked for this column.
    levels: frozenset[BuildingLevel] = frozenset()
    #: *En étage min/max*, the minimum. A grid that prints ``2/6`` requires two.
    floors_min: int = 0
    #: *Largeur du terrain min (m)*. ``None`` where the grid prints ``-``.
    min_lot_width_m: float | None = None
    #: *Nombre de logements maximal*. ``None`` where the grid leaves it blank.
    max_dwellings: int | None = None
    #: *Densité min/max* - gross floor area over lot area.
    density_min: float | None = None
    density_max: float | None = None
    #: *Taux d'implantation au sol min/max (%)* - footprint over lot area.
    site_coverage_min_pct: float | None = None
    site_coverage_max_pct: float | None = None
    #: The zone this column belongs to, carried for reporting only.
    zone: str | None = None

    def __post_init__(self) -> None:
        if self.floors_max < 0:
            raise ProgramError(f"{self!r}: floors_max must not be negative")
        if self.floors_min < 0:
            raise ProgramError(f"{self!r}: floors_min must not be negative")
        for name in ("density_min", "density_max"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ProgramError(f"{self!r}: {name} must not be negative")
        for name in ("site_coverage_min_pct", "site_coverage_max_pct"):
            value = getattr(self, name)
            if value is not None and not 0 <= value <= 100:
                raise ProgramError(f"{self!r}: {name} must be a percentage")

    @property
    def permits_residential(self) -> bool:
        return any(is_residential_usage(usage) for usage in self.usages)

    @property
    def permits_commercial(self) -> bool:
        return any(is_commercial_usage(usage) for usage in self.usages)

    @property
    def permits_industrial(self) -> bool:
        return any(is_industrial_usage(usage) for usage in self.usages)

    @property
    def permitted_floors_count(self) -> int:
        """Storeys the level rows allow this column's usages to occupy.

        One number for the whole column, and that is the grid's own doing: the
        *Niveaux de batiment autorises* block is marked per column, not per
        usage code, so a column headed ``H.2, C.2`` says where "H.2 and C.2"
        may go and never which of the two belongs on the ground floor. The
        model therefore lets any of the column's usages occupy any of the
        storeys it allows. A grid that means "commerce at the RDC, housing
        above" prints that as two columns, and two columns are two envelopes.
        """
        return permitted_floors(self.levels, self.floors_max)

    @property
    def residential_floors(self) -> int:
        """Storeys this column's usage may occupy."""
        return self.permitted_floors_count


@dataclass(frozen=True)
class Lot:
    """The parcel, as the two upstream measurements leave it.

    ``frontage_m`` is what `lot_frontage` computes - the longest street edge,
    `frontage_rank = 1` - and is the width the grid's *Largeur du terrain min*
    is tested against. They are not the same measurement in principle (a lot's
    width and its street frontage differ on a wedge-shaped parcel) and this
    treats them as one, which is the approximation the frontage asset exists to
    support.
    """

    area_m2: float
    frontage_m: float
    lot_number: str | None = None

    def __post_init__(self) -> None:
        if self.area_m2 <= 0:
            raise ProgramError(f"lot area must be positive, got {self.area_m2}")
        if self.frontage_m < 0:
            raise ProgramError(f"frontage must not be negative, got {self.frontage_m}")


@dataclass(frozen=True)
class ParkingRules:
    """What the dwellings owe in stalls, and what a stall costs to provide.

    Two rows of numbers that look alike and are not. The *areas* are what a
    stall takes out of a floor plate, and the underground one is larger; the
    *costs* are what a stall takes out of the objective, and the underground
    one is also larger. Underground is worse on both counts and is still
    routinely the right answer, because the third column - the one that has no
    field here because it is the by-law's rather than the builder's - is that
    an underground stall consumes no *superficie de plancher* and no storey.
    That exclusion is article 38 1° of 01-283 and it is applied in
    `solve_program`, not here: this dataclass is the price list, and the
    by-law is the reason the price list is worth consulting.

    Defaults are the module constants, so `ParkingRules()` is the program the
    constants describe. `NO_PARKING` is the same object with the ratio at
    zero, which drops every parking variable to a domain of ``{0}`` and leaves
    the pure-envelope question the module answered before parking existed.
    """

    #: Stalls per dwelling. Rounded **up** in aggregate, never per dwelling.
    stalls_per_dwelling: float = STALLS_PER_DWELLING
    #: Stalls per thousand square feet of commercial or industrial floor.
    #: Rounded up in the same aggregate, and *with* the dwellings rather than
    #: beside them: a building owes one number of stalls, so a half stall owed
    #: by the retail and a half owed by the housing are one stall between them
    #: and not two.
    stalls_per_1000_sqft: float = STALLS_PER_1000_SQFT
    #: Square feet a stall occupies, aisles and ramps included.
    underground_area_sqft: float = UNDERGROUND_STALL_AREA_SQFT
    above_grade_area_sqft: float = ABOVE_GRADE_STALL_AREA_SQFT
    #: Dollars of capital per stall.
    underground_cost_cad: float = UNDERGROUND_STALL_COST_CAD
    above_grade_cost_cad: float = ABOVE_GRADE_STALL_COST_CAD
    #: Months that capital is spread over to be subtractable from a rent.
    amortization_months: int = AMORTIZATION_MONTHS
    #: The deepest the model may dig. A modelling bound, not a norm.
    max_underground_levels: int = MAX_UNDERGROUND_LEVELS

    def __post_init__(self) -> None:
        if self.stalls_per_dwelling < 0:
            raise ProgramError(
                f"stalls_per_dwelling must not be negative, "
                f"got {self.stalls_per_dwelling}"
            )
        if self.stalls_per_1000_sqft < 0:
            raise ProgramError(
                f"stalls_per_1000_sqft must not be negative, "
                f"got {self.stalls_per_1000_sqft}"
            )
        for name in (
            "underground_area_sqft",
            "above_grade_area_sqft",
            "underground_cost_cad",
            "above_grade_cost_cad",
        ):
            value = getattr(self, name)
            if value < 0:
                raise ProgramError(f"{name} must not be negative, got {value}")
        if self.amortization_months <= 0:
            raise ProgramError(
                f"amortization_months must be positive, got {self.amortization_months}"
            )
        if self.max_underground_levels < 0:
            raise ProgramError(
                f"max_underground_levels must not be negative, "
                f"got {self.max_underground_levels}"
            )

    @property
    def required(self) -> bool:
        """Whether anything the building might hold owes a stall at all.

        Either ratio is enough. A column that authorises only commerce owes
        stalls without a dwelling anywhere in the program, and reading this off
        `stalls_per_dwelling` alone would let it park them nowhere.
        """
        return self.stalls_per_dwelling > 0 or self.stalls_per_1000_sqft > 0

    def monthly_cost(self, *, underground: int, above_grade: int) -> float:
        """What this many stalls costs a month, at the stated horizon."""
        capital = self.capital_cost(underground=underground, above_grade=above_grade)
        return capital / self.amortization_months

    def capital_cost(self, *, underground: int, above_grade: int) -> float:
        """What this many stalls costs to build, in dollars."""
        return (
            underground * self.underground_cost_cad
            + above_grade * self.above_grade_cost_cad
        )


#: The program the module constants describe: half a stall a dwelling, priced
#: and sized as the cost guide and the by-law leave them.
DEFAULT_PARKING = ParkingRules()

#: No stalls owed by anything, and none priced - the envelope question on its
#: own. Both ratios, so a column that authorises commerce is asked the same
#: question a bare Habitation column is.
NO_PARKING = ParkingRules(stalls_per_dwelling=0.0, stalls_per_1000_sqft=0.0)


@dataclass(frozen=True)
class ConstructionCosts:
    """What the dwellings cost to build, per square foot of dwelling.

    Charged against the *unit* schedule rather than against the gross floor
    area, which is what `UNIT_AREAS_SQFT` is doing on this side of the ledger:
    the corridors, lobbies, stairs and shafts that the difference between the
    two would pay for are not priced here at all. That understates the build
    on any real plan and is the assumption to revisit before this number is
    taken to a lender.

    The parking is **not** in here. A stall is priced per stall by
    `ParkingRules`, the way the cost guide prices it, and adding a per-square-
    foot charge on top would bill the same concrete twice.
    """

    #: Dollars per square foot of dwelling.
    residential_cost_per_sqft: float = RESIDENTIAL_COST_PER_SQFT_CAD
    #: Dollars per square foot of commercial and of industrial floor. Charged
    #: against the *gross* area of those storeys rather than against a unit
    #: schedule, which is the difference noted at the constants above: there is
    #: no `UNIT_AREAS_SQFT` for retail, so there is no leasable-versus-gross
    #: gap to fall through, and the revenue is charged on the same area.
    commercial_cost_per_sqft: float = COMMERCIAL_COST_PER_SQFT_CAD
    industrial_cost_per_sqft: float = INDUSTRIAL_COST_PER_SQFT_CAD
    #: Months that capital is spread over to be subtractable from a rent.
    amortization_months: int = AMORTIZATION_MONTHS

    def __post_init__(self) -> None:
        for name in (
            "residential_cost_per_sqft",
            "commercial_cost_per_sqft",
            "industrial_cost_per_sqft",
        ):
            value = getattr(self, name)
            if value < 0:
                raise ProgramError(f"{name} must not be negative, got {value}")
        if self.amortization_months <= 0:
            raise ProgramError(
                f"amortization_months must be positive, got {self.amortization_months}"
            )

    def capital_cost(self, area_sqft: float) -> float:
        """What this many square feet of dwelling costs to build, in dollars."""
        return area_sqft * self.residential_cost_per_sqft

    def monthly_cost(self, area_sqft: float) -> float:
        """The same, charged against a month of rent at the stated horizon."""
        return self.capital_cost(area_sqft) / self.amortization_months

    def commercial_capital_cost(self, area_sqft: float) -> float:
        """What this many square feet of commercial floor costs, in dollars."""
        return area_sqft * self.commercial_cost_per_sqft

    def commercial_monthly_cost(self, area_sqft: float) -> float:
        """The same, at the stated horizon."""
        return self.commercial_capital_cost(area_sqft) / self.amortization_months

    def industrial_capital_cost(self, area_sqft: float) -> float:
        """What this many square feet of industrial floor costs, in dollars."""
        return area_sqft * self.industrial_cost_per_sqft

    def industrial_monthly_cost(self, area_sqft: float) -> float:
        """The same, at the stated horizon."""
        return self.industrial_capital_cost(area_sqft) / self.amortization_months


#: The build the module constants describe.
DEFAULT_CONSTRUCTION = ConstructionCosts()

#: Space that costs nothing to put up - the revenue side on its own. All three
#: rates, so a test that pins a rent is not quietly still paying for a retail
#: floor.
NO_CONSTRUCTION_COST = ConstructionCosts(
    residential_cost_per_sqft=0.0,
    commercial_cost_per_sqft=0.0,
    industrial_cost_per_sqft=0.0,
)


@dataclass(frozen=True)
class UnitEconomics:
    """What a dwelling of each class earns, per CMHC's published grids.

    Both maps are keyed by CMHC bedroom class and both are read exactly as
    `average_rents` and `vacancy_rates` write them: rent in dollars a month,
    vacancy **in percent**.

    A class CMHC suppressed - `status` of `suppressed` or `no_units`, written
    as null - simply has no key here. That is not an error and is not a zero:
    it means the survey will not price that class in this neighbourhood, so the
    solver is not allowed to build it and says so in
    `DevelopmentProgram.unpriced_types`. Treating a suppressed cell as $0 rent
    would silently rule the class out; treating it as free would let the model
    build a tower of them.
    """

    average_rent_cad: Mapping[str, float]
    vacancy_rate_pct: Mapping[str, float] = field(default_factory=dict)

    def monthly_revenue(self, unit_type: str) -> float | None:
        """What one dwelling of ``unit_type`` collects in a month, or ``None``.

        ``rent x (1 - vacancy)``: effective gross income for a single
        dwelling, in the dollars CMHC published it in. Not scaled by area -
        the rent already knows how big the dwelling is, because CMHC surveys
        by bedroom class - and scaling it again is what made the old objective
        a proxy rather than a number. Size earns its keep on the cost side, in
        `ConstructionCosts`.

        This is revenue, not income: `solve_program` is where the build cost
        is netted off it, and the module docstring lists what is netted off
        neither.
        """
        rent = self.average_rent_cad.get(unit_type)
        if rent is None:
            return None
        vacancy_pct = self.vacancy_rate_pct.get(unit_type)
        occupancy = 1.0 - (vacancy_pct or 0.0) / 100.0
        return rent * occupancy


@dataclass(frozen=True)
class NonResidentialEconomics:
    """What a square foot of commerce or industry earns, per year.

    The counterpart of `UnitEconomics` for the space that is not dwellings,
    and deliberately a different shape rather than a fifth key in that map.
    Two things differ and either would be a silent error if the two were
    merged: the unit is a square foot and not a dwelling, and the period is a
    year and not a month. The third element is the same on both sides -
    `rent x (1 - vacancy)` - and it is spelled the same way here, in percent
    as published vacancy is written, so a reader holding both objects does not
    have to remember which one is a fraction.

    Where the vacancy comes from is what still differs: CMHC surveys the
    dwelling figure and this one is stated. `COMMERCIAL_VACANCY_PCT` says what
    the module assumes and why, and a caller with a broker's number for the
    submarket should pass it.

    Zero on either rate is how a caller says "not this one" without reaching
    for the zoning: `NO_NON_RESIDENTIAL` is that, for both.
    """

    #: Dollars per square foot per year, asking - the vacancy below is what
    #: makes it effective.
    commercial_per_sqft_year: float = COMMERCIAL_REVENUE_PER_SQFT_CAD
    industrial_per_sqft_year: float = INDUSTRIAL_REVENUE_PER_SQFT_CAD
    #: Share of the floor earning nothing, **in percent**.
    commercial_vacancy_pct: float = COMMERCIAL_VACANCY_PCT
    industrial_vacancy_pct: float = INDUSTRIAL_VACANCY_PCT
    #: Months in the year the rates above are quoted over. A field rather than
    #: a bare 12 so the conversion is visible at the call site of a proforma
    #: that quotes monthly rates instead.
    months_per_year: int = MONTHS_PER_YEAR

    def __post_init__(self) -> None:
        for name in ("commercial_per_sqft_year", "industrial_per_sqft_year"):
            value = getattr(self, name)
            if value < 0:
                raise ProgramError(f"{name} must not be negative, got {value}")
        for name in ("commercial_vacancy_pct", "industrial_vacancy_pct"):
            value = getattr(self, name)
            if not 0 <= value <= 100:
                raise ProgramError(f"{name} must be a percentage, got {value}")
        if self.months_per_year <= 0:
            raise ProgramError(
                f"months_per_year must be positive, got {self.months_per_year}"
            )

    @property
    def commercial_per_sqft_month(self) -> float:
        """Asking rent a month. Not what the floor earns - see below."""
        return self.commercial_per_sqft_year / self.months_per_year

    @property
    def industrial_per_sqft_month(self) -> float:
        """Asking rent a month. Not what the floor earns - see below."""
        return self.industrial_per_sqft_year / self.months_per_year

    @property
    def commercial_effective_per_sqft_month(self) -> float:
        """`rent x (1 - vacancy)` for a square foot of commerce, a month.

        The figure the objective is built on, and the one to compare against a
        dwelling's `UnitEconomics.monthly_revenue`.
        """
        return self.commercial_per_sqft_month * (
            1.0 - self.commercial_vacancy_pct / 100.0
        )

    @property
    def industrial_effective_per_sqft_month(self) -> float:
        """`rent x (1 - vacancy)` for a square foot of industry, a month."""
        return self.industrial_per_sqft_month * (
            1.0 - self.industrial_vacancy_pct / 100.0
        )

    def commercial_monthly_revenue(self, area_sqft: float) -> float:
        """What this much commercial floor collects in a month, vacancy off."""
        return area_sqft * self.commercial_effective_per_sqft_month

    def industrial_monthly_revenue(self, area_sqft: float) -> float:
        """What this much industrial floor collects in a month, vacancy off."""
        return area_sqft * self.industrial_effective_per_sqft_month


#: The rents the module constants describe.
DEFAULT_NON_RESIDENTIAL = NonResidentialEconomics()

#: Space that earns nothing, which is how the all-residential question is asked
#: of a column that happens to authorise commerce as well. Not the same as a
#: column that forbids it: the variables still exist and the solver still
#: declines to build, because at zero rent every square foot of it is a loss.
NO_NON_RESIDENTIAL = NonResidentialEconomics(
    commercial_per_sqft_year=0.0, industrial_per_sqft_year=0.0
)


@dataclass(frozen=True)
class DevelopmentProgram:
    """The mix the solver settled on, and the envelope it fills."""

    #: Dwellings by CMHC bedroom class. Classes the solver declined to build
    #: are absent rather than zero.
    units: Mapping[str, int]
    #: Storeys **above grade**, residential and parking together. This is the
    #: number *En étage* is tested against; the underground levels are not
    #: storeys and are counted separately.
    floors: int
    footprint_m2: float
    #: The *superficie de plancher* the density index is computed on:
    #: `footprint x floors`, above grade only. The underground levels are
    #: excluded by article 38 1° and are in `underground_area_m2` instead.
    gross_floor_area_m2: float
    #: Floor area the dwellings actually occupy; at most the gross above.
    unit_area_m2: float
    #: The objective: monthly net operating income. Revenue over the mix,
    #: less the amortised cost of building the dwellings and the stalls. See
    #: the module docstring for the long list of things it does *not* net out.
    net_operating_income: float
    status: str
    #: The storey split behind `floors`. Residential floors carry the mix;
    #: parking floors carry stalls and nothing else; the other two carry the
    #: usages the column authorises beside the housing, and are zero on a
    #: column that authorises none.
    residential_floors: int = 0
    above_grade_parking_floors: int = 0
    commercial_floors: int = 0
    industrial_floors: int = 0
    #: Levels dug below grade. Not storeys, and not floor area.
    underground_levels: int = 0
    #: Stalls, by where they were put. Their sum meets the dwellings' demand.
    underground_stalls: int = 0
    above_grade_stalls: int = 0
    #: `footprint x underground_levels` - built, paid for, and invisible to
    #: both the density cap and the storey cap.
    underground_area_m2: float = 0.0
    #: `footprint x commercial_floors` and `footprint x industrial_floors`.
    #: Above grade, so both are inside `gross_floor_area_m2` and both counted
    #: against *Densite* - they are floor area the by-law sees, unlike the
    #: underground parking beside them.
    commercial_area_m2: float = 0.0
    industrial_area_m2: float = 0.0
    #: What the stalls cost to build, in dollars. A capital figure, unlike
    #: `net_operating_income`, which is monthly.
    parking_cost_cad: float = 0.0
    #: What the dwellings cost to build, in dollars. Capital, and priced off
    #: `UNIT_AREAS_SQFT` rather than off the gross floor area.
    construction_cost_cad: float = 0.0
    #: What the commercial and industrial storeys cost to build, in dollars.
    #: Capital, and priced off their gross area - the one above and not a unit
    #: schedule, because there is none for them.
    commercial_cost_cad: float = 0.0
    industrial_cost_cad: float = 0.0
    #: Effective gross income a month, before either cost is taken off it.
    #: The objective above is this, less the two of them amortised.
    gross_revenue_cad: float = 0.0
    #: Which cap the answer is sitting against, for whoever asks why it is not
    #: bigger. More than one can bind at once.
    binding: tuple[str, ...] = ()
    #: Classes left out because CMHC published no rent for them.
    unpriced_types: tuple[str, ...] = ()
    zone: str | None = None
    lot_number: str | None = None

    @property
    def total_dwellings(self) -> int:
        return sum(self.units.values())

    @property
    def total_stalls(self) -> int:
        return self.underground_stalls + self.above_grade_stalls

    @property
    def commercial_area_sqft(self) -> float:
        return self.commercial_area_m2 / M2_PER_SQFT

    @property
    def industrial_area_sqft(self) -> float:
        return self.industrial_area_m2 / M2_PER_SQFT

    @property
    def non_residential_area_m2(self) -> float:
        """The commerce and the industry together. Parking is not in here -
        it is reported by where it was put, and the underground half of it is
        not floor area at all."""
        return self.commercial_area_m2 + self.industrial_area_m2

    @property
    def total_capital_cost_cad(self) -> float:
        """Dwellings, non-residential floors and stalls together, in dollars."""
        return (
            self.construction_cost_cad
            + self.commercial_cost_cad
            + self.industrial_cost_cad
            + self.parking_cost_cad
        )

    @property
    def profit(self) -> float:
        """`net_operating_income`, under the name it used to carry."""
        return self.net_operating_income

    @property
    def solved(self) -> bool:
        return self.status in ("OPTIMAL", "FEASIBLE")


def select_residential_column(
    columns: Sequence[ZoneColumn], frontage_m: float
) -> ZoneColumn | None:
    """The Habitation column that governs a lot of this width.

    A grid can authorise dwellings in more than one column, and the columns are
    then distinguished by the parcel they apply to: *Largeur du terrain min*
    rises across them, so the widest minimum the lot still satisfies is the
    column written for a lot of its size. Columns the lot is too narrow for are
    not applicable; among the rest, the most specific wins.

    A minimum is a floor, so a lot exactly as wide as one **meets** it - the
    test is ``min_lot_width_m <= frontage_m``. A column printing ``-`` has no
    minimum and is the fallback every lot qualifies for.

    Returns ``None`` when the grid authorises no dwelling at all, or when every
    residential column demands more frontage than the lot has.
    """
    eligible = [
        column
        for column in columns
        if column.permits_residential
        and (column.min_lot_width_m is None or column.min_lot_width_m <= frontage_m)
    ]
    if not eligible:
        return None
    return max(eligible, key=lambda column: column.min_lot_width_m or 0.0)


def solve_program(
    column: ZoneColumn,
    lot: Lot,
    economics: UnitEconomics,
    *,
    unit_areas_sqft: Mapping[str, float] = UNIT_AREAS_SQFT,
    parking: ParkingRules = DEFAULT_PARKING,
    construction: ConstructionCosts = DEFAULT_CONSTRUCTION,
    non_residential: NonResidentialEconomics = DEFAULT_NON_RESIDENTIAL,
    max_seconds: float = 10.0,
) -> DevelopmentProgram:
    """The program maximising monthly net operating income on ``lot``.

    The model, in square metres:

    ====================================  ===================================
    ``footprint``                         within *Taux d'implantation* x lot
                                          area, and shared by every floor
    ``residential_floors``                within *En étage min* .. storeys the
                                          level rows allow this usage
    ``parking_floors``                    above-grade parking storeys, 0 or
                                          more
    ``commercial_floors``                 0 unless the column's usages include
                                          a ``C`` class
    ``industrial_floors``                 0 unless they include an ``I`` class
    ``floors``                            the four added up, within
                                          *En étage max*
    ``underground_levels``                0 .. `max_underground_levels`; not
                                          storeys
    ``residential_area``                  ``footprint x residential_floors``
    ``parking_area``                      ``footprint x parking_floors``
    ``commercial_area``                   ``footprint x commercial_floors``
    ``industrial_area``                   ``footprint x industrial_floors``
    ``underground_area``                  ``footprint x underground_levels``
    ``gross``                             the four above-grade areas, within
                                          *Densité* x lot area
    ``sum(area_t x n_t) <= residential``  the dwellings fit in their storeys
    ``sum(n_t) <= max_dwellings``         *Nombre de logements maximal*
    ``stalls >= n_t and area ratios``     everything's stalls are provided,
                                          in one inequality
    ``stall_area x stalls <= area``       and they fit where they were put
    ====================================  ===================================

    Five products of decision variables now rather than one, and they share
    the ``footprint`` factor, which is what makes ``gross`` a plain sum of four
    of them instead of a sixth multiplication.

    **What the density cap does and does not see.** ``gross`` is above-grade
    area only. ``underground_area`` is built and paid for and appears in
    neither the *Densité* constraint nor the storey count, which is article
    38 1° of 01-283 - *une aire de stationnement des véhicules [...] située en
    sous-sol, de même que leurs voies d'accès* - expressed as the one place
    the two parking options stop being interchangeable.

    **Why the choice is not trivial.** An underground stall is bigger and
    dearer than an above-grade one, so with a slack envelope the solver puts
    the parking above grade and pockets the difference. Tighten *Densité*
    until a parking storey costs a dwelling worth more than the price gap and
    it digs instead. Both behaviours are the model working.

    Commerce and industry sharpen that choice rather than adding to it. At
    `stalls_per_1000_sqft` a non-residential plate owes roughly its own area
    back in stalls, so above grade it costs two storeys of envelope to get
    one storey of rent - and the density cap, which the retail floor is
    already inside, has to hold both. Underground, only the retail floor
    counts. A mixed-use column with a tight *Densité* is where the model digs
    first.

    **What is being maximised.** Each dwelling contributes
    ``rent x (1 - vacancy)`` a month less its share of what it cost to build,
    ``area_sqft x residential_cost_per_sqft / amortization_months``; each
    stall subtracts its own price on the same footing. Both costs are per
    dwelling and per stall respectively, so they fold into the objective's
    coefficients and the program stays the same shape it was.

    A square foot of commerce or of industry folds in the same way, one step
    further out: its rent and its build cost are both per square foot, so the
    pair collapses to a single coefficient on the *area* variable rather than
    on a count. At the module's own rates a square foot of commerce nets
    ``80/12 - 300/300`` = $5.67 a month and a square foot of industry
    ``30/12 - 200/300`` = $1.83, against a one-bedroom's $470 a month over
    600 square feet, or $0.78 a foot. Commerce therefore outbids housing for a
    storey wherever the grid allows it, industry does not, and both answers
    are the arithmetic rather than a preference - which is the reason to be
    able to change all four rates from the call site.

    **Where the non-residential storeys go.** Nowhere in particular: they are
    plates in the same stack, and the model has no notion of *which* storey a
    plate is. A grid saying "commerce at the RDC, housing above" says it in
    two columns with different *Niveaux* rows, and two columns are two
    envelopes - see `ZoneColumn.permitted_floors_count`. Within one column the
    storeys are interchangeable, which overstates what a single mixed-use
    column can really be built as, and is the assumption to revisit before a
    mixed-use answer is taken seriously.

    A class whose rent does not cover its construction contributes a negative
    coefficient and the solver declines to build it, and the same is true of a
    commercial or industrial storey. A parcel where that is true of everything
    comes back with no dwellings and no floors at all, which is an answer about
    the parcel: at these rents and these rates, nothing pencils.

    Every cap is skipped where the grid does not state one. All storeys are
    identical, and a storey is dwellings, parking, commerce or industry and
    never two of them - both by assumption, and both the reason the envelope
    collapses to a single footprint and a handful of counts.

    A minimum - on density, coverage or storeys - can make the model
    infeasible, and so now can the parking: a lot with no room for the stalls
    its dwellings owe has no program at this ratio. That is a real answer
    about the parcel rather than a bug, and `NO_PARKING` is how to ask the
    question without it. The status says which.
    """
    if not column.permits_residential:
        raise ProgramError(
            f"{column.usages} authorises no dwelling; "
            "select_residential_column is what picks a column that does"
        )

    floors_allowed = column.residential_floors
    if floors_allowed < column.floors_min:
        # Not an empty solution but a contradiction between two rows of the
        # same column, so it is named rather than returned as a bare
        # INFEASIBLE the caller would have to diagnose. *En étage min* is read
        # as a minimum on the *dwellings* storeys, not on the building's, so a
        # parking floor is not allowed to talk the grid out of it.
        return _empty_program(
            column,
            lot,
            status="INFEASIBLE",
            binding=("floors_min_exceeds_permitted_levels",),
        )

    unpriced = tuple(
        sorted(
            unit_type
            for unit_type in unit_areas_sqft
            if economics.monthly_revenue(unit_type) is None
        )
    )
    priced = {
        unit_type: area_sqft
        for unit_type, area_sqft in unit_areas_sqft.items()
        if unit_type not in unpriced
    }
    if not priced:
        return _empty_program(
            column,
            lot,
            status="INFEASIBLE",
            binding=("no_priced_unit_type",),
            unpriced=unpriced,
        )

    footprint_lo = _ceil_scaled(
        lot.area_m2 * (column.site_coverage_min_pct or 0.0) / 100.0
    )
    footprint_hi = _floor_scaled(
        lot.area_m2
        * (
            100.0
            if column.site_coverage_max_pct is None
            else column.site_coverage_max_pct
        )
        / 100.0
    )
    if footprint_lo > footprint_hi:
        return _empty_program(
            column,
            lot,
            status="INFEASIBLE",
            binding=("site_coverage_range",),
            unpriced=unpriced,
        )

    density_cap = (
        _floor_scaled(lot.area_m2 * column.density_max)
        if column.density_max is not None
        else None
    )

    # Two ceilings, and confusing them is the easy mistake. The *building* may
    # stack `floors_max` storeys of any kind; the *dwellings* may only occupy
    # the storeys the level rows allow. The first sizes the area variables,
    # the second sizes the unit counts.
    residential_hi = footprint_hi * floors_allowed
    if density_cap is not None:
        residential_hi = min(residential_hi, density_cap)

    # A storey that is not dwellings has to fit inside `floors_max` alongside
    # them, and the dwellings already claim `floors_min` of them. Commerce and
    # industry are bounded by the level rows on top of that, the same way the
    # dwellings are - they are usages of this column, and the rows are the
    # column's.
    spare_floors = max(column.floors_max - column.floors_min, 0)
    parking_floors_hi = spare_floors
    commercial_floors_hi = (
        min(spare_floors, floors_allowed) if column.permits_commercial else 0
    )
    industrial_floors_hi = (
        min(spare_floors, floors_allowed) if column.permits_industrial else 0
    )
    underground_levels_hi = parking.max_underground_levels if parking.required else 0

    smallest_unit_area = min(
        _scale_area(area_sqft * M2_PER_SQFT) for area_sqft in priced.values()
    )
    dwellings_hi = int(residential_hi // smallest_unit_area)
    if column.max_dwellings is not None:
        dwellings_hi = min(dwellings_hi, column.max_dwellings)

    # The two ratios, each expressed against the thing it is charged on: one
    # per dwelling, one per hundredth of a square metre of non-residential
    # floor. `STALL_DEMAND_SCALE` is what lets them be added together.
    stall_ratio = round(parking.stalls_per_dwelling * STALL_DEMAND_SCALE)
    area_stall_ratio = round(
        parking.stalls_per_1000_sqft
        / 1000.0
        / (AREA_SCALE * M2_PER_SQFT)
        * STALL_DEMAND_SCALE
    )

    # The most non-residential floor this envelope could hold, which is what
    # bounds the stalls it could owe. Both caps apply: the storeys the level
    # rows spare, and the density the grid prints.
    non_residential_area_hi = footprint_hi * (
        commercial_floors_hi + industrial_floors_hi
    )
    if density_cap is not None:
        non_residential_area_hi = min(non_residential_area_hi, density_cap)

    # Ceiling division: a part stall is a stall, which is how a ratio is read
    # everywhere one is written into a by-law.
    stalls_hi = -(
        -(stall_ratio * dwellings_hi + area_stall_ratio * non_residential_area_hi)
        // STALL_DEMAND_SCALE
    )
    underground_stall_area = _scale_area(parking.underground_area_sqft * M2_PER_SQFT)
    above_grade_stall_area = _scale_area(parking.above_grade_area_sqft * M2_PER_SQFT)

    model = cp_model.CpModel()

    footprint = model.NewIntVar(footprint_lo, footprint_hi, "footprint")
    residential_floors = model.NewIntVar(
        column.floors_min, floors_allowed, "residential_floors"
    )
    parking_floors = model.NewIntVar(0, parking_floors_hi, "parking_floors")
    commercial_floors = model.NewIntVar(0, commercial_floors_hi, "commercial_floors")
    industrial_floors = model.NewIntVar(0, industrial_floors_hi, "industrial_floors")
    underground_levels = model.NewIntVar(0, underground_levels_hi, "underground_levels")
    floors = model.NewIntVar(column.floors_min, column.floors_max, "floors")
    model.Add(
        floors
        == residential_floors + parking_floors + commercial_floors + industrial_floors
    )
    # The level rows are the *column's*, not any one usage's, so the storeys
    # its usages occupy between them are bounded by what those rows allow -
    # not by that number three times over. A column marked "Tous sauf le RDC"
    # on a six-storey grid authorises five storeys of housing-or-commerce, and
    # without this the solver would stack two of housing on four of commerce
    # and hand back a building standing on a ground floor nothing may occupy.
    #
    # The parking is *not* in this sum: a stall is not one of the usages the
    # grid marks these rows for, and the storey it sits in is bounded by
    # *En etage max* alone - which is the constraint above.
    model.Add(
        residential_floors + commercial_floors + industrial_floors <= floors_allowed
    )

    # The products that rule out an LP: every floor type is a footprint the
    # model chooses, stacked a number of times the model also chooses. All
    # three share `footprint`, which is the "every floor is the same plate"
    # assumption stated as algebra.
    residential_area = model.NewIntVar(
        0, max(footprint_hi * floors_allowed, 0), "residential_area"
    )
    model.AddMultiplicationEquality(residential_area, [footprint, residential_floors])

    parking_area = model.NewIntVar(
        0, max(footprint_hi * parking_floors_hi, 0), "parking_area"
    )
    model.AddMultiplicationEquality(parking_area, [footprint, parking_floors])

    commercial_area = model.NewIntVar(
        0, max(footprint_hi * commercial_floors_hi, 0), "commercial_area"
    )
    model.AddMultiplicationEquality(commercial_area, [footprint, commercial_floors])

    industrial_area = model.NewIntVar(
        0, max(footprint_hi * industrial_floors_hi, 0), "industrial_area"
    )
    model.AddMultiplicationEquality(industrial_area, [footprint, industrial_floors])

    underground_area = model.NewIntVar(
        0, max(footprint_hi * underground_levels_hi, 0), "underground_area"
    )
    model.AddMultiplicationEquality(underground_area, [footprint, underground_levels])

    # `gross` is the *superficie de plancher*: above grade only, article 38 1°.
    # Commerce and industry are in it - they are storeys, and nothing in the
    # by-law excludes a storey for what is done inside it.
    gross = model.NewIntVar(0, max(footprint_hi * column.floors_max, 0), "gross")
    model.Add(
        gross == residential_area + parking_area + commercial_area + industrial_area
    )

    if density_cap is not None:
        model.Add(gross <= density_cap)
    if column.density_min is not None:
        model.Add(gross >= _ceil_scaled(lot.area_m2 * column.density_min))

    counts: dict[str, cp_model.IntVar] = {}
    for unit_type, area_sqft in priced.items():
        area = _scale_area(area_sqft * M2_PER_SQFT)
        if area <= 0:
            raise ProgramError(f"{unit_type!r} has a non-positive area")
        # Bounded by the envelope, so the domain is finite even when the grid
        # states no dwelling ceiling - an unbounded count makes CP-SAT search a
        # space it never needs to visit.
        ceiling = residential_hi // area
        if column.max_dwellings is not None:
            ceiling = min(ceiling, column.max_dwellings)
        counts[unit_type] = model.NewIntVar(0, max(int(ceiling), 0), unit_type)

    dwellings = sum(counts.values())
    model.Add(
        sum(
            _scale_area(priced[unit_type] * M2_PER_SQFT) * count
            for unit_type, count in counts.items()
        )
        <= residential_area
    )
    if column.max_dwellings is not None:
        model.Add(dwellings <= column.max_dwellings)

    underground_stalls = model.NewIntVar(0, stalls_hi, "underground_stalls")
    above_grade_stalls = model.NewIntVar(0, stalls_hi, "above_grade_stalls")
    # Scaled rather than divided, so half a stall a dwelling stays exact and
    # the remainder rounds the way a by-law rounds it - up. One inequality for
    # both demands rather than one apiece, because the building owes a single
    # number of stalls: a half stall owed by the retail and a half owed by the
    # housing are one stall between them, and two constraints would buy two.
    #
    # This is where the commerce stops being free floor area. Three stalls per
    # thousand square feet against a stall's own 300 is very nearly a parking
    # plate for every retail plate, and a parking plate is a storey *En etage*
    # counts and *Densite* counts - so a retail floor that pays for itself
    # above grade has to earn its neighbour's keep too, and the solver may
    # prefer to dig, or to build less of it.
    model.Add(
        STALL_DEMAND_SCALE * (underground_stalls + above_grade_stalls)
        >= stall_ratio * dwellings
        + area_stall_ratio * (commercial_area + industrial_area)
    )
    model.Add(underground_stall_area * underground_stalls <= underground_area)
    model.Add(above_grade_stall_area * above_grade_stalls <= parking_area)
    # A parking level exists to hold stalls. Without this the solver would be
    # free to raise an empty one to satisfy a density *minimum* - floor area
    # the by-law would count and nobody would ever build.
    model.Add(parking_floors <= above_grade_stalls)
    model.Add(underground_levels <= underground_stalls)

    underground_monthly = round(
        parking.underground_cost_cad / parking.amortization_months * MONEY_SCALE
    )
    above_grade_monthly = round(
        parking.above_grade_cost_cad / parking.amortization_months * MONEY_SCALE
    )
    # Revenue less build cost, per dwelling, folded into one coefficient a
    # class. Rounded once, at the coefficient, rather than separately on each
    # side - two roundings of a cent apiece would show up as a tie broken the
    # wrong way between classes this close together.
    net_monthly = {
        unit_type: round(
            (
                economics.monthly_revenue(unit_type)
                - construction.monthly_cost(area_sqft)
            )
            * MONEY_SCALE
        )
        for unit_type, area_sqft in priced.items()
    }
    # The same fold, one level of aggregation up: rent and build cost are both
    # per square foot for these, so the pair is a single coefficient on the
    # area variable. `_area_coefficient` is where the per-square-foot figure
    # meets the hundredths of a square metre the area is held in.
    commercial_net = _area_coefficient(
        non_residential.commercial_effective_per_sqft_month
        - construction.commercial_monthly_cost(1.0)
    )
    industrial_net = _area_coefficient(
        non_residential.industrial_effective_per_sqft_month
        - construction.industrial_monthly_cost(1.0)
    )
    model.Maximize(
        sum(net_monthly[unit_type] * count for unit_type, count in counts.items())
        + commercial_net * commercial_area
        + industrial_net * industrial_area
        - underground_monthly * underground_stalls
        - above_grade_monthly * above_grade_stalls
    )

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = max_seconds
    status = solver.Solve(model)
    status_name = solver.StatusName(status)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return _empty_program(
            column, lot, status=status_name, binding=(), unpriced=unpriced
        )

    units = {
        unit_type: solver.Value(count)
        for unit_type, count in counts.items()
        if solver.Value(count) > 0
    }
    chosen_parking_area = solver.Value(parking_area)
    chosen_commercial_area = solver.Value(commercial_area)
    chosen_industrial_area = solver.Value(industrial_area)
    commercial_sqft = _unscale(chosen_commercial_area) / M2_PER_SQFT
    industrial_sqft = _unscale(chosen_industrial_area) / M2_PER_SQFT
    chosen_underground_levels = solver.Value(underground_levels)
    chosen_underground_stalls = solver.Value(underground_stalls)
    chosen_above_grade_stalls = solver.Value(above_grade_stalls)
    unit_area = sum(
        _scale_area(priced[unit_type] * M2_PER_SQFT) * quantity
        for unit_type, quantity in units.items()
    )

    return DevelopmentProgram(
        units=units,
        floors=solver.Value(floors),
        footprint_m2=_unscale(solver.Value(footprint)),
        gross_floor_area_m2=_unscale(solver.Value(gross)),
        unit_area_m2=_unscale(unit_area),
        net_operating_income=solver.ObjectiveValue() / MONEY_SCALE,
        status=status_name,
        residential_floors=solver.Value(residential_floors),
        above_grade_parking_floors=solver.Value(parking_floors),
        commercial_floors=solver.Value(commercial_floors),
        industrial_floors=solver.Value(industrial_floors),
        commercial_area_m2=_unscale(chosen_commercial_area),
        industrial_area_m2=_unscale(chosen_industrial_area),
        underground_levels=chosen_underground_levels,
        underground_stalls=chosen_underground_stalls,
        above_grade_stalls=chosen_above_grade_stalls,
        underground_area_m2=_unscale(solver.Value(underground_area)),
        parking_cost_cad=parking.capital_cost(
            underground=chosen_underground_stalls,
            above_grade=chosen_above_grade_stalls,
        ),
        construction_cost_cad=sum(
            construction.capital_cost(priced[unit_type]) * quantity
            for unit_type, quantity in units.items()
        ),
        commercial_cost_cad=construction.commercial_capital_cost(commercial_sqft),
        industrial_cost_cad=construction.industrial_capital_cost(industrial_sqft),
        gross_revenue_cad=sum(
            economics.monthly_revenue(unit_type) * quantity
            for unit_type, quantity in units.items()
        )
        + non_residential.commercial_monthly_revenue(commercial_sqft)
        + non_residential.industrial_monthly_revenue(industrial_sqft),
        binding=_binding_caps(
            column,
            lot,
            total_dwellings=sum(units.values()),
            unit_area=unit_area,
            parking_area=chosen_parking_area,
            commercial_area=chosen_commercial_area,
            industrial_area=chosen_industrial_area,
            residential_floors=solver.Value(residential_floors),
            density_cap=density_cap,
            footprint_hi=footprint_hi,
            floors_allowed=floors_allowed,
            smallest_unit_area=smallest_unit_area,
            underground_levels=chosen_underground_levels,
            underground_levels_hi=underground_levels_hi,
        ),
        unpriced_types=unpriced,
        zone=column.zone,
        lot_number=lot.lot_number,
    )


def _binding_caps(
    column: ZoneColumn,
    lot: Lot,
    *,
    total_dwellings: int,
    unit_area: int,
    parking_area: int,
    commercial_area: int,
    industrial_area: int,
    residential_floors: int,
    density_cap: int | None,
    footprint_hi: int,
    floors_allowed: int,
    smallest_unit_area: int,
    underground_levels: int,
    underground_levels_hi: int,
) -> tuple[str, ...]:
    """Which caps the answer is pressed against.

    Reported rather than inferred by the reader, because "why is this only
    twelve units" is the first question asked of any number this module
    returns, and the candidate answers are not distinguishable from the program
    alone.

    Computed from the *caps* rather than from the solution, which matters
    because the solution is not unique in the dimensions nobody is paying for.
    Once the mix is fixed, any footprint x storeys product big enough to hold
    it is equally optimal - 13 dwellings needing 1 393.5 m2 fit just as well
    under a 280 m2 x 5 envelope as under 278.7 m2 x 5 - so reading "is the
    footprint at its maximum" off the chosen point reports whichever tied
    solution CP-SAT happened to return. What is actually true of the parcel is
    whether one more dwelling *could* have been placed, and which printed norm
    stopped it.

    The solution values that do belong here are the above-grade storeys that
    are not dwellings: the parking, the commerce and the industry. Each is
    floor area the by-law counts and the solver chose to spend on something
    else, so what is left of the density cap after them is a genuine ceiling on
    the dwellings rather than an artefact of a tie. `max_underground_levels` is
    reported on the same footing, with the caveat that it is this module's
    bound rather than anything the grid prints.

    `commercial_floor_area` and `industrial_floor_area` are reported on a
    condition of their own rather than inside the density branch, because they
    can be the answer while no printed norm binds at all: storeys the level
    rows would have allowed the dwellings, spent on space that outbid them.
    A caller reading one of them is being told that the housing figure is small
    for a reason that is in the rates rather than in the grid, which is a
    different answer from "the envelope is full" and the only one the grid
    cannot give.
    """
    binding: list[str] = []
    if column.max_dwellings is not None and total_dwellings >= column.max_dwellings:
        binding.append("max_dwellings")

    envelope_cap = footprint_hi * floors_allowed
    non_residential_area = parking_area + commercial_area + industrial_area
    remaining_density = (
        None if density_cap is None else density_cap - non_residential_area
    )
    residential_cap = (
        envelope_cap
        if remaining_density is None
        else min(envelope_cap, remaining_density)
    )
    if residential_cap - unit_area < smallest_unit_area:
        # No further dwelling of any priced class fits in the largest envelope
        # the grid allows, so the envelope is what stops the mix. Which norm
        # produced that envelope is the useful half of the answer.
        if remaining_density is not None and remaining_density <= envelope_cap:
            binding.append("density_max")
            if parking_area > 0:
                binding.append("above_grade_parking")
        if remaining_density is None or envelope_cap <= remaining_density:
            binding.append("site_coverage_max")
            binding.append("floors")

    if residential_floors < floors_allowed:
        if commercial_area > 0:
            binding.append("commercial_floor_area")
        if industrial_area > 0:
            binding.append("industrial_floor_area")

    if underground_levels_hi and underground_levels >= underground_levels_hi:
        binding.append("max_underground_levels")
    return tuple(binding)


def _empty_program(
    column: ZoneColumn,
    lot: Lot,
    *,
    status: str,
    binding: tuple[str, ...],
    unpriced: tuple[str, ...] = (),
) -> DevelopmentProgram:
    return DevelopmentProgram(
        units={},
        floors=0,
        footprint_m2=0.0,
        gross_floor_area_m2=0.0,
        unit_area_m2=0.0,
        net_operating_income=0.0,
        status=status,
        binding=binding,
        unpriced_types=unpriced,
        zone=column.zone,
        lot_number=lot.lot_number,
    )


def _scale_area(m2: float) -> int:
    return round(m2 * AREA_SCALE)


def _floor_scaled(m2: float) -> int:
    return math.floor(m2 * AREA_SCALE)


def _ceil_scaled(m2: float) -> int:
    return math.ceil(m2 * AREA_SCALE)


def _unscale(scaled: int) -> float:
    return scaled / AREA_SCALE


def _area_coefficient(per_sqft_month: float) -> int:
    """Dollars a square foot a month, as an objective coefficient.

    The area variables are hundredths of a square metre and the objective is
    ten-thousandths of a dollar, so this is the whole of the conversion between
    a rate a leasing agent would quote and a number CP-SAT can multiply. Signed
    on purpose: a rate whose build cost exceeds its rent comes through negative
    and the solver declines the storey, which is the same behaviour a dwelling
    class gets from `net_monthly`.
    """
    return round(per_sqft_month / M2_PER_SQFT / AREA_SCALE * MONEY_SCALE)
