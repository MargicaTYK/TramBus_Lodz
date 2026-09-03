# Łódź Live Transit — personal commute tracker

A small self-hosted app that shows live bus/tram positions and delays in Łódź,
built on the city's official GTFS-RT feed (Zarząd Dróg i Transportu w Łodzi,
via otwarte.miasto.lodz.pl).

## Why it works this way

- `server.py` polls the live feed every 20 seconds (configurable), joins it
  against the static schedule (routes/stops/trip headsigns), and serves a
  clean JSON API at `/api/vehicles`.
- `static/index.html` is the map + list UI. It polls that local API every
  20 seconds — it never talks to the city's server directly, so you're only
  making one polite request to them regardless of how many browser tabs
  you have open.
- If a poll fails (feed hiccup, network blip), the server keeps serving the
  last good data and flags it `"stale": true` after 90 seconds, so the UI
  can be honest about it instead of showing a frozen bus as if it's live.

## Run it

```bash
pip install -r requirements.txt
python server.py
```

Then open **http://localhost:5000** in your browser.

First launch downloads the ~25 MB static GTFS file (routes/stops/trips) —
give it a few seconds before vehicles appear. After that it's cached and
only re-downloaded once every 24 hours.

## Verified before you run it

I tested the actual parsing/joining/serving logic in this file against the
real snapshot files you uploaded earlier (not fabricated data) — it correctly
returned all 347 vehicles with routes, delays, and next stops matched up. The
one thing I could *not* test from my end is the live network fetch itself,
since my sandbox can only reach a fixed allowlist of domains and the city's
server isn't on it. That call is plain `requests.get()` — nothing exotic —
but if it fails when you run it, the first thing to check is whether the
exact file paths under `/wp-content/uploads/2025/06/` have moved (city
portals occasionally reorganize).

## License / attribution

Łódź's open data portal permits commercial use of this data provided the
source is credited. The frontend footer already credits ZDiT / Portal
Otwartych Danych Miasta Łodzi — keep that in place if you build on this.

## Known gaps in this feed (so you don't go looking for these elsewhere)

- No wheelchair/vehicle-equipment data (A/C, ticket machine, USB) — the
  `wheelchair_accessible` field is empty for every trip in the static GTFS.
  If you want that feature later, it'd need a separately maintained
  vehicle-fleet lookup, not this feed.
- `alerts.bin` was empty in the sample — the Alerts feed exists but had no
  active disruptions at the time.

## Next steps (not built yet)

- Scope this down to *your* specific stop(s) and line(s) instead of the
  whole city — tell me which ones and I'll add a "my stops" view.
- Push notifications when your bus is about to arrive.
- Turn this into an installable PWA (manifest + service worker) so it lives
  on your phone's home screen like czynaczas does.
