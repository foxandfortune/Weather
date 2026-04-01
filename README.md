# Kalshi Weather Tracker

Tracks NWS hourly forecasts vs. NCEI observed highs for Kalshi high-temperature markets.

## Repo structure

```
stations.csv                  # add new locations here
R/
  pull_forecasts.R            # NWS hourly forecast capture
  pull_actuals.R              # NCEI CDO official daily highs
data/
  forecasts_KHOU.csv          # one file per station
  actuals_KHOU.csv
.github/workflows/
  pull_forecasts.yml          # runs every 2 hours
  pull_actuals.yml            # runs daily + supports manual backfill
```

## Setup

### 1. Fork / create this repo on GitHub

### 2. Add your NCEI token as a GitHub Secret

Go to **Settings → Secrets and variables → Actions → New repository secret**:

- Name: `NCEI_TOKEN`
- Value: your token from https://www.ncei.noaa.gov/cdo-web/token

### 3. Backfill historical actuals

Trigger the **Pull Actuals** workflow manually from the GitHub Actions tab:
- Set `start_date` to `2025-01-01`
- Leave `end_date` blank (defaults to yesterday)

This will populate `data/actuals_KHOU.csv` with every official daily high since Jan 1, 2025.

### 4. Enable the scheduled workflows

GitHub requires at least one push to the default branch before scheduled workflows activate. Push a small change (e.g., edit this README) to trigger them.

---

## Adding a new station

Add a row to `stations.csv`:

```
station_id,name,lat,lon,ncei_ghcnd_id,timezone
KHOU,Houston Hobby,29.6454,-95.2789,USW00012918,America/Chicago
KDFW,Dallas Fort Worth,32.8998,-97.0403,USW00003927,America/Chicago
KORD,Chicago O'Hare,41.9742,-87.9073,USW00094846,America/Chicago
```

The NCEI GHCND station ID can be found by searching https://www.ncei.noaa.gov/access/past-weather/ — look for the station name and use the ID shown (strip the `GHCND:` prefix).

On the next workflow run, new CSVs will be created automatically for any new stations.

---

## Output format

### `data/forecasts_KHOU.csv`
| Column | Description |
|---|---|
| `station_id` | NWS station ID |
| `station_name` | Human-readable name |
| `forecast_issued_utc` | When this forecast was captured |
| `forecast_for_date` | The date being forecast |
| `forecast_high_f` | Predicted high (°F) — max of all hourly temps that day |
| `n_hours_in_forecast` | How many hourly periods were available for that date |

### `data/actuals_KHOU.csv`
| Column | Description |
|---|---|
| `station_id` | Station ID |
| `station_name` | Human-readable name |
| `date` | Observation date |
| `observed_high_f` | Official TMAX from NCEI GHCND (°F) |

---

## Notes

- **Forecast lag**: NWS forecasts for "tomorrow" are typically stable by mid-afternoon. The 2-hour cadence captures how the forecast evolves through the day prior.
- **Actuals lag**: NCEI CDO data typically appears 1–4 days after the observation date. The daily workflow will keep re-checking until data appears.
- **NCEI 503 errors**: The CDO API is occasionally slow or down. The script retries once automatically; failures are non-fatal and the next daily run will catch up.
