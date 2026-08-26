"""Client for the ZEF construction cost estimator, a static publication of the
Altus Group Canadian Cost Guide.

Unlike every other source here, this one publishes nothing to fetch as data:
it is a browser tool, and its cost table ships as a JavaScript file the page
loads. `data/building-types.js` declares two array literals - ``CITIES``, nine
Canadian markets, and ``TYPES``, one entry per building type carrying a
``rates`` map of ``city -> [low, high]`` in dollars. That file is the source,
so it is read directly rather than through the page, and parsed here.

The literals are JavaScript, not JSON: keys are unquoted and strings may be
single quoted. `parse_catalog` converts them rather than evaluating them - the
file is a third party's and could grow a call or a template string - which is
why the scanner below quotes bare keys instead of running anything.

Only the Montreal column is kept by the assets that read this, the same
posture `urban_rag.cmhc` takes toward a nation-wide survey: the slice is a
bound on what was asked for, not an interpretation of what came back.

Deliberately free of Dagster imports, mirroring `urban_rag.open_data`,
`urban_rag.infolot` and `urban_rag.bdoi`.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from urban_rag.spectrum import USER_AGENT, default_ca_bundle

#: https://zef-builds.github.io/construction-estimator/
DEFAULT_BASE_URL = "https://zef-builds.github.io/construction-estimator"

#: The one script the cost table lives in. The page loads a dozen others -
#: tabs, compute, defaults - and none of them carry rates.
CATALOG_PATH = "data/building-types.js"

#: The `CITIES` id for Montreal. The guide prices nine markets; this pipeline
#: is about one island.
MONTREAL_CITY_ID = "mtl"

#: `cat` values, as the publisher spells them. Split the way the two bronze
#: assets in `urban_rag.estimator_assets` are split; `institutional` and
#: `infrastructure` are published too and are read by neither.
RESIDENTIAL_CATEGORIES: tuple[str, ...] = ("residential",)
NON_RESIDENTIAL_CATEGORIES: tuple[str, ...] = ("commercial", "industrial", "parking")

#: `TYPES` ids a proforma reads a rate off, named here rather than in the asset
#: that reads them because they are facts about the publisher's catalog - the
#: same kind of thing `MONTREAL_CITY_ID` is - and because a downstream column
#: named after one of them is a promise the id still exists upstream.
#:
#: The two parking ids are the pair a building actually chooses between: stalls
#: dug out under it, or a garage integrated into it at grade. The guide prices a
#: third, `surface_lot`, and it is deliberately not here - an asphalt lot is not
#: a parking structure, and pricing one against the other would compare a
#: building decision with the decision not to build.
UNDERGROUND_PARKING_TYPE_ID = "parkade_ug"
INTEGRATED_PARKING_TYPE_ID = "parkade_ag"
PARKING_TYPE_IDS: tuple[str, ...] = (
    UNDERGROUND_PARKING_TYPE_ID,
    INTEGRATED_PARKING_TYPE_ID,
)

#: The `residential` types labelled "Condominium / Apartment", plus the wood
#: frame band the guide files under a label of its own - one entry per storey
#: band, ascending. Townhouses, single family, seniors housing and student
#: residences share the category and are *not* here: they are priced per square
#: foot the same way, but none of them is what "an apartment building on this
#: lot" means.
#:
#: `condo_wood` leads because it leads in storeys, not because it is the
#: default. Which band a reader should take is a judgement, and it lives in
#: `LotProfilesConfig` where judgements go.
CONDO_TYPE_IDS: tuple[str, ...] = (
    "condo_wood",
    "condo_12",
    "condo_13_39",
    "condo_40_60",
    "condo_60plus",
)

#: The condo band a lot profile flattens onto a column of its own unless it is
#: configured otherwise. Wood frame up to six storeys: the construction a
#: low-rise borough of triplexes actually builds, and the cheapest square foot
#: the guide prices for a multi-unit building.
LOW_RISE_CONDO_TYPE_ID = "condo_wood"

#: Optional keys on a `TYPES` entry saying what one dollar figure buys. At most
#: one is ever set, and *none* being set is the common case - the file's own
#: header says an entry with no flag is priced per square foot. Carried through
#: as a value rather than as five sparse booleans; see `rates_frame`.
UNIT_FLAGS: tuple[str, ...] = ("perStall", "perSM", "perLM", "perUnit", "perAcre")


class EstimatorError(RuntimeError):
    """The script could not be fetched, or is not shaped the way it was."""


@dataclass(frozen=True)
class City:
    """One `CITIES` entry: a market the guide prices."""

    id: str
    label: str
    prov: str | None = None


@dataclass(frozen=True)
class BuildingType:
    """One `TYPES` entry, with its rates for every city the guide prices."""

    id: str
    label: str
    sector: str | None
    cat: str | None
    #: ``city id -> (low, high)``, in dollars per `unit_flag` - per square foot
    #: when that is `None`.
    rates: dict[str, tuple[float, float]]
    unit_flag: str | None = None
    source_note: str | None = None


@dataclass(frozen=True)
class Catalog:
    """Everything `data/building-types.js` declares, in the order it declares it."""

    cities: tuple[City, ...]
    types: tuple[BuildingType, ...]

    def city(self, city_id: str) -> City:
        for city in self.cities:
            if city.id == city_id:
                return city
        priced = ", ".join(sorted(c.id for c in self.cities))
        raise EstimatorError(f"The guide prices no city {city_id!r}; it prices: {priced}")

    def in_categories(self, categories: tuple[str, ...]) -> tuple[BuildingType, ...]:
        """Every type in ``categories``, in the guide's own declaration order."""
        wanted = set(categories)
        return tuple(t for t in self.types if t.cat in wanted)


class EstimatorClient:
    """Fetches the cost script and hands back the catalog it declares.

    Same posture as the other clients here: paced and patient rather than
    fast. This one is a static file on GitHub Pages rather than a live
    municipal server, so the pacing costs nothing worth saving.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        timeout_seconds: float = 60.0,
        request_delay_seconds: float = 0.25,
        max_retries: int = 3,
        ca_bundle: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.request_delay_seconds = request_delay_seconds
        self._session = session or self._build_session(max_retries, ca_bundle)

    @staticmethod
    def _build_session(max_retries: int, ca_bundle: str | None) -> requests.Session:
        session = requests.Session()
        bundle = ca_bundle or default_ca_bundle()
        if bundle:
            session.verify = bundle
        retry = Retry(
            total=max_retries,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers["User-Agent"] = USER_AGENT
        return session

    @property
    def catalog_url(self) -> str:
        return f"{self.base_url}/{CATALOG_PATH}"

    def fetch_source(self) -> tuple[str, str | None]:
        """The script's text, and the ``Last-Modified`` it was served with.

        Decoded as UTF-8 explicitly rather than through ``response.text``:
        requests falls back to ISO-8859-1 when a ``text/*`` response carries no
        charset, and the guide's labels are full of en dashes ("13-39 Storeys")
        that would arrive mojibaked the day the publisher drops the charset.
        """
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        try:
            response = self._session.get(self.catalog_url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise EstimatorError(f"{self.catalog_url}: {exc}") from exc

        try:
            source = response.content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise EstimatorError(f"{self.catalog_url}: not UTF-8 ({exc})") from exc
        return source, response.headers.get("Last-Modified")

    def catalog(self) -> tuple[Catalog, str | None]:
        """The parsed catalog, and the ``Last-Modified`` it was served with."""
        source, last_modified = self.fetch_source()
        return parse_catalog(source), last_modified


def parse_catalog(source: str) -> Catalog:
    """The `CITIES` and `TYPES` literals declared in ``source``."""
    cities = tuple(_city(entry) for entry in _decode(source, "CITIES"))
    types = tuple(_building_type(entry) for entry in _decode(source, "TYPES"))
    if not cities or not types:
        raise EstimatorError(
            f"The catalog script declares {len(cities)} cit(ies) and "
            f"{len(types)} type(s); it has been emptied or restructured."
        )
    return Catalog(cities=cities, types=types)


def rates_frame(
    catalog: Catalog,
    city_id: str,
    categories: tuple[str, ...],
    *,
    extra_columns: dict[str, object] | None = None,
) -> pd.DataFrame:
    """One row per published type in ``categories``, priced for ``city_id``.

    Column names are the publisher's own keys wherever the publisher has one
    (`id`, `label`, `sector`, `cat`, `sourceNote`), so a reader can line the
    frame up against `data/building-types.js` without a crosswalk. Three have
    no name upstream: `rate_low` and `rate_high` are the two ends of the
    `rates` pair the city slice picks, and `unit_flag` holds the name of
    whichever `UNIT_FLAGS` key was set - `None` meaning per square foot, which
    is what the script's own header says an entry with no flag means.

    Nothing else is derived. In particular the storey band a residential type
    is priced for stays inside `label`, where the publisher put it; turning
    "Condominium / Apartment (13-39 Storeys)" into a pair of integers is
    reading the label, which is silver's job rather than this layer's.
    """
    city = catalog.city(city_id)
    selected = catalog.in_categories(categories)
    if not selected:
        published = ", ".join(sorted({t.cat for t in catalog.types if t.cat}))
        raise EstimatorError(
            f"The guide publishes no type in {', '.join(categories)}; "
            f"its categories are: {published}"
        )

    records: list[dict[str, object]] = []
    for building in selected:
        try:
            low, high = building.rates[city_id]
        except KeyError:
            priced = ", ".join(sorted(building.rates))
            raise EstimatorError(
                f"{building.id!r} carries no rate for {city_id!r}; "
                f"it is priced for: {priced}"
            ) from None
        records.append(
            {
                "id": building.id,
                "label": building.label,
                "sector": building.sector,
                "cat": building.cat,
                "unit_flag": building.unit_flag,
                "sourceNote": building.source_note,
                "city": city.id,
                "city_label": city.label,
                "prov": city.prov,
                "rate_low": low,
                "rate_high": high,
            }
        )

    frame = pd.DataFrame.from_records(records)
    # Pinned rather than inferred so every partition writes the same parquet
    # schema. `sourceNote` and `unit_flag` are set on a minority of entries
    # and on none at all in some categories, and an all-empty column left to
    # Arrow lands as the `null` type - a schema that differs from the day one
    # value appears, which is exactly what a dated snapshot must not do.
    for column in _TEXT_COLUMNS:
        frame[column] = frame[column].astype("string")
    for name, value in (extra_columns or {}).items():
        frame[name] = value
    return frame


#: The columns of `rates_frame` that are text, pinned to a nullable string
#: dtype. Not the columns added through ``extra_columns``, which the caller
#: types by what it passes.
_TEXT_COLUMNS: tuple[str, ...] = (
    "id",
    "label",
    "sector",
    "cat",
    "unit_flag",
    "sourceNote",
    "city",
    "city_label",
    "prov",
)


def _decode(source: str, name: str) -> list[dict]:
    literal = _array_literal(source, name)
    try:
        decoded = json.loads(_to_json(literal))
    except json.JSONDecodeError as exc:
        raise EstimatorError(f"{name} is not readable as a JSON array: {exc}") from exc
    if not isinstance(decoded, list) or not all(isinstance(e, dict) for e in decoded):
        raise EstimatorError(f"{name} is not an array of objects")
    return decoded


def _city(entry: dict) -> City:
    try:
        return City(id=entry["id"], label=entry["label"], prov=entry.get("prov"))
    except KeyError as exc:
        raise EstimatorError(f"CITIES entry has no {exc.args[0]!r}: {entry}") from None


def _building_type(entry: dict) -> BuildingType:
    try:
        identifier = entry["id"]
        label = entry["label"]
        raw_rates = entry["rates"]
    except KeyError as exc:
        raise EstimatorError(f"TYPES entry has no {exc.args[0]!r}: {entry}") from None

    if not isinstance(raw_rates, dict) or not raw_rates:
        raise EstimatorError(f"{identifier!r}: rates is not a non-empty object")
    rates: dict[str, tuple[float, float]] = {}
    for city_id, pair in raw_rates.items():
        if not isinstance(pair, list) or len(pair) != 2:
            raise EstimatorError(
                f"{identifier!r}: rates.{city_id} is not a [low, high] pair"
            )
        try:
            rates[city_id] = (float(pair[0]), float(pair[1]))
        except (TypeError, ValueError) as exc:
            raise EstimatorError(
                f"{identifier!r}: rates.{city_id} is not numeric ({pair!r})"
            ) from exc

    # Reported rather than resolved: two flags would mean the entry claims to
    # be priced per stall *and* per linear metre, and picking one would be a
    # guess about what the dollars buy.
    flags = [flag for flag in UNIT_FLAGS if entry.get(flag)]
    if len(flags) > 1:
        raise EstimatorError(
            f"{identifier!r} sets more than one unit flag: {', '.join(flags)}"
        )

    return BuildingType(
        id=identifier,
        label=label,
        sector=entry.get("sector"),
        cat=entry.get("cat"),
        rates=rates,
        unit_flag=flags[0] if flags else None,
        source_note=entry.get("sourceNote"),
    )


def _array_literal(source: str, name: str) -> str:
    """The text of ``const <name> = [...]``, brackets matched, strings skipped."""
    match = re.search(rf"\bconst\s+{re.escape(name)}\s*=\s*\[", source)
    if match is None:
        raise EstimatorError(
            f"The catalog script does not declare `const {name} = [...]`; "
            "the publication has been restructured."
        )

    start = match.end() - 1  # the `[` itself
    depth = 0
    quote = ""
    escaped = False
    for index in range(start, len(source)):
        char = source[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = ""
            continue
        if char in "\"'":
            quote = char
        elif char in "[{":
            depth += 1
        elif char in "]}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    raise EstimatorError(f"{name}'s array literal is unterminated")


_IDENTIFIER = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")


def _to_json(literal: str) -> str:
    """A JavaScript array literal as JSON.

    Three differences, and nothing else is touched: bare keys are quoted,
    single-quoted strings are re-quoted, and a comma before a closing bracket
    is dropped. An identifier *not* followed by a colon is left alone, so
    ``true``/``false``/``null`` pass through as the JSON literals they already
    are - and anything else that reaches here (a function call, a bare
    variable) fails in `json.loads`, with its own text in the message, rather
    than being executed.
    """
    out: list[str] = []
    index = 0
    length = len(literal)
    while index < length:
        char = literal[index]
        if char in "\"'":
            end = _end_of_string(literal, index)
            chunk = literal[index:end]
            out.append(chunk if char == '"' else _requote(chunk))
            index = end
            continue
        if char in "]}":
            _drop_trailing_comma(out)
            out.append(char)
            index += 1
            continue
        identifier = _IDENTIFIER.match(literal, index)
        if identifier is not None:
            word = identifier.group()
            index = identifier.end()
            after = index
            while after < length and literal[after].isspace():
                after += 1
            out.append(json.dumps(word) if literal[after : after + 1] == ":" else word)
            continue
        out.append(char)
        index += 1
    return "".join(out)


def _end_of_string(literal: str, start: int) -> int:
    """Index just past the string literal opening at ``start``."""
    quote = literal[start]
    index = start + 1
    while index < len(literal):
        char = literal[index]
        if char == "\\":
            index += 2
            continue
        if char == quote:
            return index + 1
        index += 1
    raise EstimatorError("Unterminated string literal in the catalog script")


def _requote(chunk: str) -> str:
    """A single-quoted JS string as a double-quoted JSON one.

    Only the quoting changes: the escape sequences JSON also understands are
    left exactly as they are, so nothing inside the string is reinterpreted on
    the way through.
    """
    body = chunk[1:-1].replace("\\'", "'")
    return '"' + body.replace('"', '\\"') + '"'


def _drop_trailing_comma(out: list[str]) -> None:
    index = len(out) - 1
    while index >= 0 and out[index].isspace():
        index -= 1
    if index >= 0 and out[index] == ",":
        del out[index]
