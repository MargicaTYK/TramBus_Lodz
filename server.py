from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')
"""
Łódź live public transport — backend.

Polls the city's official GTFS-RT feeds (published by ZDiT via
otwarte.miasto.lodz.pl) and serves clean, joined JSON to the frontend.

Data source: Zarząd Dróg i Transportu w Łodzi (ZDiT), via the
Portal Otwartych Danych Miasta Łodzi. Published under an open license
that permits commercial use provided the source is credited — keep the
attribution in the frontend footer intact.

Run:
    pip install -r requirements.txt
    python server.py
Then open http://localhost:5000
"""
import csv
import io
import os
import pickle
import re
import sqlite3
import threading
import time
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, send_from_directory
from google.transit import gtfs_realtime_pb2

GTFS_URL = "https://otwarte.miasto.lodz.pl/wp-content/uploads/2025/06/GTFS.zip"
TRIP_UPDATES_URL = "https://otwarte.miasto.lodz.pl/wp-content/uploads/2025/06/trip_updates.bin"
VEHICLE_POSITIONS_URL = "https://otwarte.miasto.lodz.pl/wp-content/uploads/2025/06/vehicle_positions.bin"
ALERTS_URL = "https://otwarte.miasto.lodz.pl/wp-content/uploads/2025/06/alerts.bin"

POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", 5))
STATIC_GTFS_REFRESH_SECONDS = 24 * 60 * 60  # static schedule rarely changes; once a day is plenty
STALE_AFTER_SECONDS = 90  # if the live feed hasn't moved in this long, tell the user
_APP_DIR = os.path.dirname(os.path.abspath(__file__))  # anchor data files to server.py's own folder,
                                                          # not whatever directory the process happened
                                                          # to start in (double-click, IDE, a terminal
                                                          # opened elsewhere all differ here on Windows) —
                                                          # this is exactly what caused a real reported
                                                          # "unable to open database file" error
GTFS_CACHE_PATH = os.path.join(_APP_DIR, "gtfs_cache.pkl")  # avoid re-parsing 3.2M stop_times rows on every restart
GTFS_CACHE_SCHEMA_VERSION = 6  # bump whenever _parse_gtfs_zip's returned keys change — an old
                                # cache missing a key (e.g. route shapes added in a later update)
                                # would otherwise be silently trusted and serve incomplete data
HISTORY_DB_PATH = os.path.join(_APP_DIR, "history.db")
HISTORY_LOG_INTERVAL_SECONDS = 60  # one row per route per minute — keeps the db growth sane

# Łódź ticket prices, effective since 1 March 2025 (uml.lodz.pl / MPK Łódź).
# Zloty, (normal, discounted). Per-stop fare (MigAppka/Łódź.pl app, since Jan 2026):
# 1 zł first stop, 0.50 zł per stop for the 2nd-19th (normal fare).
TICKET_PRICES = {
    20: (4.40, 2.20),
    40: (5.60, 2.80),
    80: (6.80, 3.40),
    1440: (18.00, 9.00),  # 24-hour
}
PER_STOP_FARE_FIRST = 1.00
PER_STOP_FARE_NEXT = 0.50
PER_STOP_FARE_CAP = 10.00
TRANSFER_BUFFER_SECONDS = 5 * 60  # safety margin added before picking a ticket length
LONG_WAIT_THRESHOLD_MIN = 45  # flag connections requiring an unusually long wait, rather than hide the honesty gap

HEADERS = {
    "User-Agent": "lodz-personal-transit-app/0.1 (personal commute tool; contact: set-your-email-here)"
}

app = Flask(__name__, static_folder="static", static_url_path="")

# ---- in-memory state, refreshed by the background poller ----
_lock = threading.Lock()
_state = {
    "routes": {},          # route_id -> {"name": ..., "type": ...}
    "stops": {},           # stop_id -> stop_name
    "stop_zone": {},       # stop_id -> zone_id ("1"=Łódź, "2"/"3"=surrounding towns)
    "trip_headsign": {},   # trip_id -> headsign
    "stops_full": [],      # [{id, name, lat, lon, routes: [{name, mode}, ...]}, ...] for the stop layer
    "trip_stops": {},      # trip_id -> [(stop_id, seq, arr_sec, dep_sec), ...] ordered — for trip planning
    "route_trips": {},     # route_name -> [trip_id, ...] — for trip planning
    "shapes": {},           # shape_id -> [[lat, lon], ...] ordered — the physical route path
    "route_shape": {},      # route_name -> shape_id (most common shape for that route)
    "route_mode": {},       # route_name -> "tram" | "bus"
    "trip_service": {},     # trip_id -> service_id
    "trip_route": {},       # trip_id -> (route_name, mode)
    "trip_shape": {},       # trip_id -> shape_id, for a vehicle's exact path (more precise than a route's aggregated shapes)
    "calendar_weekly": {},  # service_id -> {days, start, end}
    "calendar_exceptions": {},  # service_id -> {date_str: "1" (added) or "2" (removed)}
    "_stop_routes_index": {},  # stop_id -> {(route_name, mode), ...} — fast lookup, derived from stops_full
    "vehicles": [],        # enriched list, see build_vehicle_list()
    "alerts": [],           # current service disruptions from the GTFS-RT Alerts feed
    "feed_timestamp": 0,   # unix ts from the RT feed header
    "last_fetch_ok": 0,    # unix ts of last successful poll
    "last_static_fetch": 0,
    "last_history_log": 0,
}


def natural_key(s):
    """Sort route codes the way a human would: '2' < '10A' < '51A' < 'Z1'."""
    return [(0, int(tok)) if tok.isdigit() else (1, tok) for tok in re.findall(r"\d+|\D+", s)]


def hms_to_seconds(hms):
    """GTFS times can exceed 24:00:00 for post-midnight trips (e.g. 25:10:00) — valid, kept as-is."""
    h, m, s = hms.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def _parse_gtfs_zip(zf):
    """Parse the GTFS zip into every index this app needs: routes, stops
    (with zone), trip headsigns, and — the expensive part — per-trip
    ordered stop sequences with times, joined from stop_times.txt
    (~3.2M rows in Łódź's feed). Built in one pass so trip planning and
    the stop-to-routes lookup share the same scan instead of two."""
    routes = {}
    with zf.open("routes.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            routes[row["route_id"]] = {
                "name": row["route_short_name"],
                "type": row["route_type"],
            }

    stops = {}
    stop_coords = {}
    stop_zone = {}
    with zf.open("stops.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            stops[row["stop_id"]] = row["stop_name"]
            stop_zone[row["stop_id"]] = row.get("zone_id", "")
            try:
                stop_coords[row["stop_id"]] = (float(row["stop_lat"]), float(row["stop_lon"]))
            except ValueError:
                pass

    trip_headsign = {}
    trip_route = {}
    trip_shape = {}
    trip_service = {}
    with zf.open("trips.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            trip_headsign[row["trip_id"]] = row["trip_headsign"]
            trip_shape[row["trip_id"]] = row.get("shape_id", "")
            trip_service[row["trip_id"]] = row.get("service_id", "")
            r = routes.get(row["route_id"])
            if r:
                trip_route[row["trip_id"]] = (r["name"], "tram" if r["type"] == "0" else "bus")

    # Single pass building stop->routes (for the map layer) AND trip->ordered
    # stop sequence with times (for trip planning). Hand-rolled split instead
    # of csv.DictReader — noticeably faster over 3.2M rows, and we only need
    # 5 of the 10 columns.
    stop_routes_raw = defaultdict(set)
    trip_stops_raw = defaultdict(list)
    with zf.open("stop_times.txt") as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig")
        next(text)  # header
        for line in text:
            parts = line.rstrip("\r\n").split(",")
            if len(parts) < 5:
                continue
            trip_id, arr, dep, stop_id, seq = parts[0], parts[1], parts[2], parts[3], parts[4]
            rt = trip_route.get(trip_id)
            if not rt:
                continue
            stop_routes_raw[stop_id].add(rt)
            try:
                trip_stops_raw[trip_id].append((stop_id, int(seq), hms_to_seconds(arr), hms_to_seconds(dep)))
            except ValueError:
                continue

    for tid in trip_stops_raw:
        trip_stops_raw[tid].sort(key=lambda x: x[1])

    route_trips = defaultdict(list)
    for tid, (rname, mode) in trip_route.items():
        if tid in trip_stops_raw:
            route_trips[rname].append(tid)

    # Physical route path, for drawing a line on the map. A route can have
    # several shapes (both directions, branch variants) — the most common
    # one by trip count is used as "the" path, same simplification already
    # used for stop patterns.
    shapes_raw = defaultdict(list)
    with zf.open("shapes.txt") as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig")
        next(text)  # header
        for line in text:
            parts = line.rstrip("\r\n").split(",")
            if len(parts) < 4:
                continue
            shape_id, lat, lon, seq = parts[0], parts[1], parts[2], parts[3]
            try:
                shapes_raw[shape_id].append((int(seq), float(lat), float(lon)))
            except ValueError:
                continue
    for sid in shapes_raw:
        shapes_raw[sid].sort(key=lambda x: x[0])
    shapes = {sid: [[lat, lon] for _, lat, lon in pts] for sid, pts in shapes_raw.items()}

    # Which service_ids are active on a given date — needed so a stop's
    # timetable shows only trips that actually run that day, not a jumbled
    # mix of weekday/Saturday/Sunday service together. Confirmed by reading
    # this feed's own calendar.txt: every weekday flag is 0 for every
    # service_id — activation happens entirely through calendar_dates.txt
    # date exceptions instead. Implemented per the full GTFS spec anyway
    # (weekday flags + exceptions layered on top), so this stays correct
    # even if a future feed update uses the weekday-flag style instead.
    weekday_names = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    calendar_weekly = {}  # service_id -> {"days": [bool*7], "start": "YYYYMMDD", "end": "YYYYMMDD"}
    if "calendar.txt" in zf.namelist():
        with zf.open("calendar.txt") as f:
            for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
                calendar_weekly[row["service_id"]] = {
                    "days": [row[d] == "1" for d in weekday_names],
                    "start": row["start_date"], "end": row["end_date"],
                }
    calendar_exceptions = defaultdict(dict)  # service_id -> {date_str: "1" or "2"}
    if "calendar_dates.txt" in zf.namelist():
        with zf.open("calendar_dates.txt") as f:
            for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
                calendar_exceptions[row["service_id"]][row["date"]] = row["exception_type"]

    # A route's most common single shape is usually just ONE direction —
    # confirmed on a real case (tram 12): drawing only the top shape covered
    # 30 of 93 real stops the route actually serves, because the reverse
    # direction has its own stops. Grouping by STOP-SEQUENCE PATTERN rather
    # than raw shape_id, because shape_id itself isn't a reliable proxy for
    # "distinct physical path" in this feed — confirmed two different
    # shape_ids were byte-for-byte identical geometry, apparently reissued
    # per calendar/service-day grouping even when nothing physically
    # changed. Taking enough top patterns to cover 85% of trips (capped at
    # 4) covers the real network far more honestly.
    route_shape = {}
    route_mode = {}
    for rname, trip_ids in route_trips.items():
        pattern_groups = defaultdict(list)
        for tid in trip_ids:
            seq = trip_stops_raw.get(tid)
            if seq:
                pattern_groups[tuple(s[0] for s in seq)].append(tid)
        if pattern_groups:
            total = sum(len(tids) for tids in pattern_groups.values())
            selected_shapes = []
            seen_shapes = set()
            cumulative = 0
            for pattern, tids in sorted(pattern_groups.items(), key=lambda kv: -len(kv[1])):
                cumulative += len(tids)
                sid = trip_shape.get(tids[0])
                if sid and sid not in seen_shapes:
                    seen_shapes.add(sid)
                    selected_shapes.append(sid)
                if cumulative / total >= 0.85 or len(selected_shapes) >= 4:
                    break
            route_shape[rname] = selected_shapes
        first_mode = next((trip_route[tid][1] for tid in trip_ids if tid in trip_route), None)
        if first_mode:
            route_mode[rname] = first_mode

    stops_full = []
    for stop_id, name in stops.items():
        coords = stop_coords.get(stop_id)
        if not coords:
            continue
        route_pairs = sorted(stop_routes_raw.get(stop_id, set()), key=lambda rt: natural_key(rt[0]))
        stops_full.append({
            "id": stop_id,
            "name": name,
            "lat": round(coords[0], 6),
            "lon": round(coords[1], 6),
            "routes": [{"name": r, "mode": m} for r, m in route_pairs],
        })

    return {
        "routes": routes,
        "stops": stops,
        "stop_zone": stop_zone,
        "trip_headsign": trip_headsign,
        "stops_full": stops_full,
        "trip_stops": dict(trip_stops_raw),
        "route_trips": dict(route_trips),
        "shapes": shapes,
        "route_shape": route_shape,
        "route_mode": route_mode,
        "trip_service": trip_service,
        "trip_route": trip_route,
        "trip_shape": trip_shape,
        "calendar_weekly": calendar_weekly,
        "calendar_exceptions": dict(calendar_exceptions),
    }


def load_static_gtfs():
    """Load the static GTFS data, using a local disk cache (good for
    STATIC_GTFS_REFRESH_SECONDS) so restarting the app during normal use
    doesn't re-download and re-parse 3.2M rows every time — that parse
    alone takes ~15s, which is a lot to eat on every restart.

    The cache is tagged with GTFS_CACHE_SCHEMA_VERSION. Without this, an
    old cache file (written before a code update added new parsed fields,
    e.g. route shapes) would still look "fresh" by age alone and get
    silently trusted — serving genuinely incomplete data with no error.
    Confirmed as the real cause of a "route lines don't draw" report: the
    cached data simply had no shape fields in it at all."""
    parsed = None
    if os.path.exists(GTFS_CACHE_PATH) and (time.time() - os.path.getmtime(GTFS_CACHE_PATH)) < STATIC_GTFS_REFRESH_SECONDS:
        try:
            with open(GTFS_CACHE_PATH, "rb") as f:
                cached = pickle.load(f)
            if cached.get("_schema_version") == GTFS_CACHE_SCHEMA_VERSION:
                parsed = cached["data"]
                print("[static] loaded from local cache (gtfs_cache.pkl)")
            else:
                print("[static] cache is from an older code version, ignoring and re-parsing")
        except Exception as exc:
            print(f"[static] cache read failed ({exc}), re-downloading")

    if parsed is None:
        resp = requests.get(GTFS_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        parsed = _parse_gtfs_zip(zf)
        try:
            with open(GTFS_CACHE_PATH, "wb") as f:
                pickle.dump({"_schema_version": GTFS_CACHE_SCHEMA_VERSION, "data": parsed}, f)
        except Exception as exc:
            print(f"[static] failed to write cache: {exc}")

    with _lock:
        _state.update(parsed)
        _state["_stop_routes_index"] = {
            s["id"]: {(r["name"], r["mode"]) for r in s["routes"]} for s in parsed["stops_full"]
        }
        _state["last_static_fetch"] = time.time()

    print(f"[static] loaded {len(parsed['routes'])} routes, {len(parsed['stops'])} stops, "
          f"{len(parsed['trip_headsign'])} trips, {len(parsed['stops_full'])} stops with route assignments, "
          f"{len(parsed['trip_stops'])} trips with full stop sequences")


# ============================================================================
# Trip planner — direct routing over the static schedule, plus a ticket
# recommendation. Originally built with one-transfer routing too, but that
# was removed by choice: it needed real bug-fixing (false-positive transfer
# matching, a ranking bug that let a fast late-night ride outrank an
# immediate slower one) and still produced edge cases needing a "long wait"
# warning. Direct-only is simpler and every result here is trustworthy
# without caveats.
# ============================================================================

def active_service_ids_for_date(date_str):
    """date_str is 'YYYYMMDD'. Returns the set of service_ids actually active
    on that date — per full GTFS spec: the weekly recurring pattern first,
    then calendar_dates.txt exceptions (add/remove) layered on top. Must be
    called with _lock already held, since it reads _state directly."""
    weekday_idx = datetime.strptime(date_str, "%Y%m%d").weekday()  # 0=Monday
    active = set()
    for sid, info in _state["calendar_weekly"].items():
        if info["start"] <= date_str <= info["end"] and info["days"][weekday_idx]:
            active.add(sid)
    for sid, exceptions in _state["calendar_exceptions"].items():
        exc_type = exceptions.get(date_str)
        if exc_type == "1":
            active.add(sid)
        elif exc_type == "2":
            active.discard(sid)
    return active


def direct_options(origin, dest, after_seconds, max_options=3, route_filter=None):
    """Best direct (no-transfer) options, one per route, soonest arrival
    first. Verified: durations genuinely vary by time of day (14-17 min
    across a day for a real 51A segment), reflecting the schedule's own
    peak/off-peak timing — not fabricated.

    Ranked by absolute arrival time, not ride duration — a shorter ride
    departing much later must not outrank a longer one leaving immediately
    (confirmed and fixed during testing).

    route_filter restricts the search to one specific line — used by the
    "pick a line, then pick its stops" planner flow, so a result never
    surprises the user with a different line than the one they chose even
    if the two stops happen to share more than one route."""
    stop_routes = _state["_stop_routes_index"]
    trip_stops = _state["trip_stops"]
    route_trips = _state["route_trips"]
    common_routes = stop_routes.get(origin, set()) & stop_routes.get(dest, set())
    if route_filter:
        common_routes = {r for r in common_routes if r[0] == route_filter}
    best_per_route = {}
    for rname, mode in common_routes:
        for tid in route_trips.get(rname, []):
            seq = trip_stops.get(tid)
            if not seq:
                continue
            o_entry = next((s for s in seq if s[0] == origin), None)
            if not o_entry or o_entry[3] < after_seconds:
                continue
            d_entry = next((s for s in seq if s[0] == dest), None)
            if not d_entry or o_entry[1] >= d_entry[1]:
                continue
            if rname not in best_per_route or o_entry[3] < best_per_route[rname]["dep"]:
                best_per_route[rname] = {
                    "route": rname, "mode": mode,
                    "dep": o_entry[3], "arr": d_entry[2], "trip_id": tid,
                    "stops_traveled": d_entry[1] - o_entry[1],
                }
    results = sorted(best_per_route.values(), key=lambda l: l["arr"])
    return results[:max_options]


def average_live_delay(route_name):
    """Current average delay (seconds) across live vehicles on this route,
    if any are running right now — used to nudge the schedule-based
    estimate toward today's actual conditions. Returns None if nothing on
    this route is currently in the feed (e.g. it's not operating hours)."""
    delays = [v["delay_s"] for v in _state["vehicles"] if v["route"] == route_name and v["delay_s"] is not None]
    if not delays:
        return None
    return sum(delays) / len(delays)


def recommend_ticket(duration_seconds, stops_traveled, zones_touched):
    """Cheapest sufficient ticket, with a safety buffer so normal timing
    variance doesn't leave the ticket expiring mid-trip. Per-stop fare
    (Jan 2026 tariff) is compared for short, zone-1-only hops since it can
    be genuinely cheaper than a time ticket for a handful of stops."""
    buffered = duration_seconds + TRANSFER_BUFFER_SECONDS
    minutes = buffered / 60

    time_ticket = None
    for length_min in sorted(TICKET_PRICES):
        if minutes <= length_min:
            normal, discounted = TICKET_PRICES[length_min]
            time_ticket = {"length_min": length_min, "price_normal": normal, "price_discounted": discounted}
            break
    if time_ticket is None:
        normal, discounted = TICKET_PRICES[1440]
        time_ticket = {"length_min": 1440, "price_normal": normal, "price_discounted": discounted, "note": "trip exceeds 80 min — 24h ticket shown as a fallback"}

    per_stop_fare = None
    if zones_touched <= {"1"} and stops_traveled is not None and stops_traveled >= 1:
        cost = PER_STOP_FARE_FIRST + max(0, min(stops_traveled, 19) - 1) * PER_STOP_FARE_NEXT
        cost = min(cost, PER_STOP_FARE_CAP)
        per_stop_fare = {"stops": stops_traveled, "price_normal": round(cost, 2)}

    cheaper_option = "per_stop" if (per_stop_fare and per_stop_fare["price_normal"] < time_ticket["price_normal"]) else "time_ticket"

    return {
        "time_ticket": time_ticket,
        "per_stop_fare": per_stop_fare,
        "recommended": cheaper_option,
        "zones_touched": sorted(zones_touched),
    }



def fetch_protobuf(url):
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed


def build_vehicle_list(vp_feed, tu_feed):
    with _lock:
        routes = _state["routes"]
        stops = _state["stops"]
        trip_headsign = _state["trip_headsign"]

    # For each trip, keep every (stop_sequence, delay) pair rather than just
    # one value — delays can genuinely differ across a long trip's remaining
    # stops, and a vehicle needs the delay relevant to ITS current position,
    # not an arbitrary array position (confirmed via the GTFS-RT spec: each
    # StopTimeUpdate carries its own stop_sequence for exactly this reason).
    stop_time_updates_by_trip = {}
    for e in tu_feed.entity:
        tu = e.trip_update
        entries = []
        for stu in tu.stop_time_update:
            delay = None
            if stu.HasField("arrival"):
                delay = stu.arrival.delay
            elif stu.HasField("departure"):
                delay = stu.departure.delay
            if delay is not None:
                entries.append((stu.stop_sequence, delay))
        if entries:
            stop_time_updates_by_trip[tu.trip.trip_id] = entries

    def delay_for_vehicle(trip_id, current_seq):
        entries = stop_time_updates_by_trip.get(trip_id)
        if not entries:
            return None
        best = None
        for seq, delay in entries:
            if seq >= current_seq and (best is None or seq < best[0]):
                best = (seq, delay)
        raw = best[1] if best is not None else entries[-1][1]  # no stop at/after current position listed — fall back to the last known delay
        # Sanity clamp: a real transit delay is never plausibly more than
        # ~2 hours. If the feed reports one, something upstream is wrong —
        # treat it as unreliable rather than propagate a multi-hour error
        # into the vehicle list, its popup, and every remaining stop's ETA.
        return raw if abs(raw) <= 7200 else None

    vehicles = []
    for e in vp_feed.entity:
        v = e.vehicle
        route = routes.get(v.trip.route_id, {"name": v.trip.route_id, "type": "3"})
        vehicles.append({
            "id": v.vehicle.id,
            "trip_id": v.trip.trip_id,
            "route": route["name"],
            "mode": "tram" if route["type"] == "0" else "bus",
            "headsign": trip_headsign.get(v.trip.trip_id, ""),
            "lat": round(v.position.latitude, 6),
            "lon": round(v.position.longitude, 6),
            "speed_kmh": round(v.position.speed * 3.6, 1) if v.position.speed else 0,
            "next_stop": stops.get(v.stop_id, ""),
            "current_stop_sequence": v.current_stop_sequence,
            "delay_s": delay_for_vehicle(v.trip.trip_id, v.current_stop_sequence),
            "status": gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(v.current_status),
        })
    return vehicles


def init_history_db():
    conn = sqlite3.connect(HISTORY_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS delay_samples (
            route TEXT NOT NULL,
            hour INTEGER NOT NULL,
            weekday INTEGER NOT NULL,
            avg_delay_s REAL NOT NULL,
            sample_count INTEGER NOT NULL,
            sampled_at INTEGER NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_route_hour ON delay_samples(route, hour, weekday)")
    conn.commit()
    conn.close()


def log_history_sample():
    """Record one row per currently-running route with its live average
    delay right now. This is the 'start logging so predictions improve
    over time' piece — nothing reads this yet, since one snapshot is not
    a history. After a few weeks of this running, it becomes possible to
    ask 'how does this route usually run at this hour on this weekday',
    which today's schedule-plus-live-delay estimate cannot know."""
    by_route = defaultdict(list)
    for v in _state["vehicles"]:
        if v["delay_s"] is not None:
            by_route[v["route"]].append(v["delay_s"])
    if not by_route:
        return
    now = datetime.now()
    conn = sqlite3.connect(HISTORY_DB_PATH)
    conn.executemany(
        "INSERT INTO delay_samples (route, hour, weekday, avg_delay_s, sample_count, sampled_at) VALUES (?,?,?,?,?,?)",
        [
            (route, now.hour, now.weekday(), sum(delays) / len(delays), len(delays), int(time.time()))
            for route, delays in by_route.items()
        ],
    )
    conn.commit()
    conn.close()


def _translated_text(translated_string, preferred_lang="pl"):
    """A GTFS-RT TranslatedString can carry several language variants —
    pick Polish if present, otherwise whatever's first, otherwise empty."""
    translations = translated_string.translation
    if not translations:
        return ""
    for t in translations:
        if t.language == preferred_lang:
            return t.text
    return translations[0].text


def build_alert_list(alert_feed):
    alerts = []
    for e in alert_feed.entity:
        a = e.alert
        routes = sorted({ie.route_id for ie in a.informed_entity if ie.route_id})
        alerts.append({
            "id": e.id,
            "header": _translated_text(a.header_text),
            "description": _translated_text(a.description_text),
            "effect": gtfs_realtime_pb2.Alert.Effect.Name(a.effect) if a.effect else None,
            "cause": gtfs_realtime_pb2.Alert.Cause.Name(a.cause) if a.cause else None,
            "routes": routes,
        })
    return alerts


def poll_once():
    vp_feed = fetch_protobuf(VEHICLE_POSITIONS_URL)
    tu_feed = fetch_protobuf(TRIP_UPDATES_URL)
    vehicles = build_vehicle_list(vp_feed, tu_feed)
    with _lock:
        _state["vehicles"] = vehicles
        _state["feed_timestamp"] = vp_feed.header.timestamp
        _state["last_fetch_ok"] = time.time()
    print(f"[poll] {len(vehicles)} vehicles, feed ts {vp_feed.header.timestamp}")

    # Kept separate from the vehicle poll above: if the alerts feed ever
    # hiccups or the endpoint moves, it should never take vehicle tracking
    # down with it.
    try:
        alert_feed = fetch_protobuf(ALERTS_URL)
        alerts = build_alert_list(alert_feed)
        with _lock:
            _state["alerts"] = alerts
        if alerts:
            print(f"[alerts] {len(alerts)} active")
    except Exception as exc:
        print(f"[alerts] fetch failed: {exc}")

    if time.time() - _state["last_history_log"] >= HISTORY_LOG_INTERVAL_SECONDS:
        try:
            log_history_sample()
            _state["last_history_log"] = time.time()
        except Exception as exc:
            print(f"[history] log failed: {exc}")


def poller_loop():
    init_history_db()
    load_static_gtfs()
    while True:
        try:
            if time.time() - _state["last_static_fetch"] > STATIC_GTFS_REFRESH_SECONDS:
                load_static_gtfs()
            poll_once()
        except Exception as exc:  # keep serving last-known-good data if a poll fails
            print(f"[poll] FAILED: {exc}")
        time.sleep(POLL_INTERVAL_SECONDS)


@app.route("/api/vehicles")
def api_vehicles():
    with _lock:
        age = time.time() - _state["last_fetch_ok"] if _state["last_fetch_ok"] else None
        return jsonify({
            "vehicles": _state["vehicles"],
            "feed_timestamp": _state["feed_timestamp"],
            "age_seconds": round(age, 1) if age is not None else None,
            "stale": (age is None) or (age > STALE_AFTER_SECONDS),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        })


@app.route("/api/alerts")
def api_alerts():
    with _lock:
        return jsonify({"alerts": _state["alerts"]})


def _fmt_hm(seconds):
    seconds = seconds % (24 * 3600)
    return f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}"


def _leg_zone(stop_id):
    return _state["stop_zone"].get(stop_id, "1")


def _build_leg_response(leg, from_id, to_id):
    delay = average_live_delay(leg["route"])
    return {
        "route": leg["route"],
        "mode": leg["mode"],
        "from_stop": _state["stops"].get(from_id, from_id),
        "to_stop": _state["stops"].get(to_id, to_id),
        "dep": _fmt_hm(leg["dep"]),
        "arr": _fmt_hm(leg["arr"]),
        "scheduled_duration_min": round((leg["arr"] - leg["dep"]) / 60, 1),
        "live_avg_delay_min": round(delay / 60, 1) if delay is not None else None,
        "stops_traveled": leg.get("stops_traveled"),
    }


def _option_from_direct(leg, origin, dest, after_seconds):
    leg_resp = _build_leg_response(leg, origin, dest)
    sched_s = leg["arr"] - leg["dep"]
    delay_s = average_live_delay(leg["route"]) or 0
    estimated_s = sched_s + max(delay_s, 0)  # only ever adds expected lateness, never subtracts on a hunch
    zones = {_leg_zone(origin), _leg_zone(dest)}
    ticket = recommend_ticket(estimated_s, leg.get("stops_traveled"), zones)
    wait_before_min = (leg["dep"] - after_seconds) / 60
    return {
        "type": "direct",
        "legs": [leg_resp],
        "departs": _fmt_hm(leg["dep"]),
        "arrives": _fmt_hm(leg["arr"]),
        "wait_before_departure_min": round(wait_before_min, 1),
        "total_time_from_now_min": round((leg["arr"] - after_seconds + max(delay_s, 0)) / 60, 1),
        "scheduled_duration_min": round(sched_s / 60, 1),
        "estimated_duration_min": round(estimated_s / 60, 1),
        "live_delay_applied": leg_resp["live_avg_delay_min"] is not None,
        "long_wait_warning": wait_before_min > LONG_WAIT_THRESHOLD_MIN,
        "ticket": ticket,
    }


@app.route("/api/routes")
def api_routes():
    with _lock:
        routes = _state["routes"]
        live_routes = {v["route"] for v in _state["vehicles"]}
        seen = set()
        result = []
        for r in routes.values():
            if r["name"] in seen:
                continue
            seen.add(r["name"])
            result.append({
                "name": r["name"],
                "mode": "tram" if r["type"] == "0" else "bus",
                "active": r["name"] in live_routes,
            })
        result.sort(key=lambda r: natural_key(r["name"]))
        return jsonify({"routes": result})


@app.route("/api/route-shape")
def api_route_shape():
    from flask import request
    route_input = request.args.get("route", "").strip()
    with _lock:
        route_trips = _state["route_trips"]
        route_name = next((rn for rn in route_trips if rn.lower() == route_input.lower()), None)
        if not route_name:
            return jsonify({"error": f"Nie znaleziono linii '{route_input}'."}), 404
        shape_ids = _state["route_shape"].get(route_name, [])
        shapes = [_state["shapes"][sid] for sid in shape_ids if sid in _state["shapes"]]
        mode = _state["route_mode"].get(route_name)
        return jsonify({"route": route_name, "mode": mode, "shapes": shapes})


def seconds_until(time_sec, now_sec):
    """Signed seconds from now_sec (wall-clock, always 0-24h) until time_sec
    (a GTFS schedule time, which can exceed 24h for night-line trips
    continuing past midnight on the same service day).

    A blanket 'if the raw gap looks too big, shift by a day' rule can't
    tell apart two very different situations that produce the same large
    raw gap: a genuine night-line time (e.g. "26:51") compared against an
    equally-early now (e.g. 02:50) -- which SHOULD read as ~1 minute away,
    not ~24 hours -- versus an ordinary early-morning trip that is simply
    many hours in the past relative to a late-afternoon now, which should
    stay a large negative number, not get "corrected" into looking
    upcoming. Confirmed this exact confusion in production: a stop's
    05:04-to-17:11 gap was actually every ordinary daytime departure
    between them being wrongly hidden, while long-past early-morning
    trips got wrongly resurrected as "upcoming".

    The ambiguity only ever exists when now_sec ITSELF is in the early
    hours (before ~4am) and time_sec is a genuine night-line value (>=24h)
    -- only then do we bring time_sec down to its real 0-24h clock time
    before comparing. Every other case is a plain, unambiguous subtraction.
    """
    if time_sec >= 24 * 3600 and now_sec < 4 * 3600:
        return (time_sec - 24 * 3600) - now_sec
    return time_sec - now_sec


@app.route("/api/vehicle-detail")
def api_vehicle_detail():
    from flask import request
    trip_id = request.args.get("trip_id", "").strip()
    if not trip_id:
        return jsonify({"error": "Brak parametru trip_id."}), 400
    try:
        current_seq = int(request.args.get("current_stop_sequence", "0"))
    except ValueError:
        current_seq = 0
    try:
        delay_s = int(request.args.get("delay_s", "0"))
    except (ValueError, TypeError):
        delay_s = 0
    # Sanity clamp: a real transit delay is never plausibly more than ~2
    # hours. If it is, something upstream is wrong (a bad real-time feed
    # entry, a mismatch) — treat it as unreliable rather than propagate a
    # multi-hour error into every remaining stop's displayed ETA.
    if abs(delay_s) > 7200:
        delay_s = 0

    with _lock:
        seq = _state["trip_stops"].get(trip_id)
        if not seq:
            return jsonify({"error": "Nie znaleziono trasy dla tego pojazdu."}), 404
        stops = _state["stops"]

        now = datetime.now()
        now_sec = now.hour * 3600 + now.minute * 60 + now.second
        stop_entries = []
        # Remaining stops: everything from the vehicle's current position in the
        # sequence onward — current_stop_sequence comes straight from GTFS-RT,
        # confirmed populated on every vehicle in this feed (347 of 347 checked).
        for stop_id, stop_seq, arr_sec, dep_sec in seq:
            if stop_seq < current_seq:
                continue
            base_time = dep_sec if dep_sec is not None else arr_sec
            if base_time is None:
                continue
            eta_sec = base_time + delay_s
            diff = seconds_until(eta_sec, now_sec)
            stop_entries.append({
                "stop_id": stop_id,
                "name": stops.get(stop_id, stop_id),
                "time": f"{(eta_sec // 3600) % 24:02d}:{(eta_sec % 3600) // 60:02d}",
                "eta_min": round(diff / 60),
            })

        shape_id = _state["trip_shape"].get(trip_id)
        shape_points = _state["shapes"].get(shape_id, []) if shape_id else []

        return jsonify({"stops": stop_entries, "shape": shape_points})


@app.route("/api/network-stats")
def api_network_stats():
    with _lock:
        vehicles = _state["vehicles"]

    facts = []
    if vehicles:
        # Busiest line right now: most vehicles currently in service. An
        # honest proxy for service intensity -- NOT ridership, since the
        # open feed has no passenger-count data at all.
        by_route_count = defaultdict(int)
        for v in vehicles:
            by_route_count[v["route"]] += 1
        busiest_route, busiest_count = max(by_route_count.items(), key=lambda kv: kv[1])
        facts.append({
            "label": "Najbardziej obciążona linia teraz",
            "value": f"{busiest_route} ({busiest_count} pojazdów)",
        })

        # Average delay across the network, at this exact moment.
        delays = [v["delay_s"] for v in vehicles if v["delay_s"] is not None]
        if delays:
            avg_delay_min = sum(delays) / len(delays) / 60
            sign = "+" if avg_delay_min >= 0 else ""
            facts.append({
                "label": "Średnie opóźnienie w sieci",
                "value": f"{sign}{avg_delay_min:.1f} min",
            })

        # Most punctual line right now: smallest average delay among lines
        # with at least 2 active vehicles, so one lucky bus can't crown an
        # entire line on its own.
        by_route_delays = defaultdict(list)
        for v in vehicles:
            if v["delay_s"] is not None:
                by_route_delays[v["route"]].append(v["delay_s"])
        eligible = {r: d for r, d in by_route_delays.items() if len(d) >= 2}
        if eligible:
            best_route, best_delays = min(eligible.items(), key=lambda kv: abs(sum(kv[1]) / len(kv[1])))
            best_avg_min = sum(best_delays) / len(best_delays) / 60
            sign = "+" if best_avg_min >= 0 else ""
            facts.append({
                "label": "Najbardziej punktualna linia teraz",
                "value": f"{best_route} ({sign}{best_avg_min:.1f} min)",
            })

    return jsonify({"facts": facts})


@app.route("/api/stop-timetable")
def api_stop_timetable():
    from flask import request
    stop_ids_param = request.args.get("stop_ids", "").strip()
    date_param = request.args.get("date", "").strip()
    if not stop_ids_param:
        return jsonify({"error": "Brak parametru stop_ids."}), 400
    stop_ids = set(stop_ids_param.split(","))
    now = datetime.now()
    if not date_param:
        date_param = now.strftime("%Y%m%d")
    try:
        datetime.strptime(date_param, "%Y%m%d")
    except ValueError:
        return jsonify({"error": "Nieprawidłowy format daty (oczekiwano RRRRMMDD)."}), 400

    with _lock:
        active_services = active_service_ids_for_date(date_param)
        trip_stops = _state["trip_stops"]
        trip_service = _state["trip_service"]
        trip_route = _state["trip_route"]
        trip_headsign = _state["trip_headsign"]
        stops = _state["stops"]

        entries = []
        for tid, seq in trip_stops.items():
            if trip_service.get(tid) not in active_services:
                continue
            route = trip_route.get(tid)
            if not route:
                continue
            for sid, _stop_seq, arr_sec, dep_sec in seq:
                if sid in stop_ids:
                    entries.append({
                        "route": route[0], "mode": route[1],
                        "headsign": trip_headsign.get(tid, ""),
                        "time_sec": dep_sec if dep_sec is not None else arr_sec,
                    })

        # Only filter to "remaining today" when the requested date IS today —
        # a schedule requested for a different date should show the whole day.
        is_today = date_param == now.strftime("%Y%m%d")
        if is_today:
            now_sec = now.hour * 3600 + now.minute * 60 + now.second
            entries = [e for e in entries if seconds_until(e["time_sec"], now_sec) >= -60]  # small buffer so a just-departed entry doesn't vanish mid-render

        entries.sort(key=lambda e: e["time_sec"])
        for e in entries:
            t = e.pop("time_sec")
            e["time"] = f"{(t // 3600) % 24:02d}:{(t % 3600) // 60:02d}"
        stop_name = next((stops[s] for s in stop_ids if s in stops), None)
        return jsonify({"stop_name": stop_name, "date": date_param, "entries": entries})


@app.route("/api/route-stops")
def api_route_stops():
    from flask import request
    route_input = request.args.get("route", "").strip()
    with _lock:
        route_trips = _state["route_trips"]
        trip_stops = _state["trip_stops"]
        stops = _state["stops"]
        # Case-insensitive match: the main search box lowercases everything
        # (e.g. "51a"), while real route names are mixed case ("51A").
        route_name = next((rn for rn in route_trips if rn.lower() == route_input.lower()), None)
        trips = route_trips.get(route_name, []) if route_name else []
        if not trips:
            return jsonify({"error": f"Nie znaleziono linii '{route_input}'."}), 404

        # Combine both directions into one list without needing GTFS
        # direction_id (not currently parsed): take the most common exact
        # stop-sequence pattern as the backbone, then append any stops from
        # other real patterns not already included, in their own order.
        pattern_counts = {}
        for tid in trips:
            seq = trip_stops.get(tid)
            if not seq:
                continue
            pattern = tuple(s[0] for s in seq)
            pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1

        seen = set()
        ordered_ids = []
        for pattern, _count in sorted(pattern_counts.items(), key=lambda kv: -kv[1]):
            for sid in pattern:
                if sid not in seen:
                    seen.add(sid)
                    ordered_ids.append(sid)

        # Group by display name: a name like "Rokicińska-Rondo Inwalidów" can
        # be 2+ separate stop_ids on the same line (opposite-side platforms,
        # different bays). Showing them as separate dropdown entries with
        # identical text is unusable — confirmed directly: a real case had
        # 4 such platforms where only 1 of 4 combinations actually connects,
        # and picking the wrong one (impossible to tell apart visually) gave
        # a false "no route" answer. Grouping them and searching every real
        # combination (the /api/trip-plan endpoint already supports
        # comma-separated ids) fixes this at the root instead of hiding it.
        groups = {}
        order = []
        for sid in ordered_ids:
            name = stops.get(sid)
            if not name:
                continue
            if name not in groups:
                groups[name] = []
                order.append(name)
            groups[name].append(sid)

        result = [{"name": name, "ids": groups[name]} for name in order]
        return jsonify({"route": route_name, "stops": result})


@app.route("/api/trip-plan")
def api_trip_plan():
    from flask import request
    origin_ids = [s for s in request.args.get("from", "").split(",") if s]
    dest_ids = [s for s in request.args.get("to", "").split(",") if s]
    route_filter = request.args.get("route", "").strip() or None
    at = request.args.get("at", "")  # optional "HH:MM"; defaults to now

    with _lock:
        stops = _state["stops"]
        origin_ids = [o for o in origin_ids if o in stops]
        dest_ids = [d for d in dest_ids if d in stops]
        if not origin_ids or not dest_ids:
            return jsonify({"error": "Unknown stop id for 'from' or 'to'."}), 400

        # Some stop names cover several platforms/bays with different real
        # routes (e.g. "Piotrkowska Centrum" is 4 separate stop_ids, only
        # some of which share any route with a given origin). Rather than
        # guess one, every real combination is checked and the genuinely
        # best result wins — confirmed necessary after a same-name pair
        # silently picked two platforms with zero shared routes.
        pairs = [(o, d) for o in origin_ids for d in dest_ids if o != d]
        if not pairs:
            return jsonify({"error": "Origin and destination are the same stop."}), 400

        if at:
            try:
                h, m = at.split(":")
                after_seconds = int(h) * 3600 + int(m) * 60
            except ValueError:
                return jsonify({"error": "Invalid 'at' time, expected HH:MM."}), 400
        else:
            now = datetime.now()
            after_seconds = now.hour * 3600 + now.minute * 60 + now.second

        options = []
        for o, d in pairs:
            for leg in direct_options(o, d, after_seconds, max_options=3, route_filter=route_filter):
                options.append(_option_from_direct(leg, o, d, after_seconds))
        options.sort(key=lambda opt: opt["total_time_from_now_min"])
        options = options[:3]

        if options:
            note = None
        elif route_filter:
            note = (f"Linia {route_filter} nie łączy tych przystanków bezpośrednio o tej porze "
                    "(sprawdzono oba kierunki).")
        else:
            note = ("No direct route found between these stops for this time (every matching platform was checked). "
                     "This trip needs at least one transfer, which this simplified planner doesn't search for.")

        return jsonify({
            "from": stops[origin_ids[0]],
            "to": stops[dest_ids[0]],
            "at": _fmt_hm(after_seconds),
            "options": options,
            "note": note,
        })


@app.route("/api/stops")
def api_stops():
    with _lock:
        return jsonify({"stops": _state["stops_full"]})


@app.route('/')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

