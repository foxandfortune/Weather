## ============================================================
## pull_forecasts.R
## Captures the current NWS hourly forecast for tomorrow's
## high temp for all stations in stations.csv.
##
## Designed to run every 2 hours via GitHub Actions.
## Each run appends one row per station to data/forecasts_<STATION>.csv
##
## Output columns:
##   station_id, station_name, forecast_issued_utc,
##   forecast_for_date, forecast_high_f, n_hours_in_forecast
## ============================================================

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(dplyr)
  library(lubridate)
  library(readr)
})

# ── Config ────────────────────────────────────────────────────────────────────

STATIONS_FILE <- "stations.csv"
DATA_DIR      <- "data"

dir.create(DATA_DIR, showWarnings = FALSE)

# ── Helpers ───────────────────────────────────────────────────────────────────

nws_get <- function(url, ...) {
  GET(url,
      add_headers("User-Agent" = "KalshiWeatherTracker/1.0 (github-actions)"),
      timeout(20),
      ...)
}

parse_nws <- function(resp) {
  fromJSON(rawToChar(content(resp, as = "raw")), flatten = TRUE)
}

# ── NWS grid lookup ───────────────────────────────────────────────────────────
get_nws_grid <- function(lat, lon) {
  resp <- nws_get(sprintf("https://api.weather.gov/points/%.4f,%.4f", lat, lon))
  if (status_code(resp) != 200) stop(sprintf("Grid lookup failed: HTTP %d", status_code(resp)))
  props <- parse_nws(resp)$properties
  list(office = props$gridId, gridX = props$gridX, gridY = props$gridY)
}

# ── Hourly forecast → tomorrow's high ─────────────────────────────────────────
get_forecast_for_date <- function(grid, target_date, tz) {
  url  <- sprintf("https://api.weather.gov/gridpoints/%s/%d,%d/forecast/hourly",
                  grid$office, grid$gridX, grid$gridY)
  resp <- nws_get(url)
  if (status_code(resp) != 200) stop(sprintf("Forecast fetch failed: HTTP %d", status_code(resp)))

  periods <- parse_nws(resp)$properties$periods

  df <- periods %>%
    select(startTime, temperature, temperatureUnit) %>%
    mutate(
      start_dt      = ymd_hms(startTime, tz = "UTC") %>% with_tz(tz),
      forecast_date = as_date(start_dt)
    ) %>%
    filter(forecast_date == target_date)

  if (nrow(df) == 0) return(NULL)

  list(
    forecast_high_f     = max(df$temperature, na.rm = TRUE),
    n_hours_in_forecast = nrow(df)
  )
}

# ── Main loop over stations ───────────────────────────────────────────────────
stations <- read_csv(STATIONS_FILE, show_col_types = FALSE)

issued_utc <- format(now("UTC"), "%Y-%m-%d %H:%M UTC")
message(sprintf("\n=== Forecast pull: %s ===", issued_utc))

for (i in seq_len(nrow(stations))) {
  st <- stations[i, ]
  message(sprintf("\n--- %s (%s) ---", st$name, st$station_id))

  outfile     <- file.path(DATA_DIR, sprintf("forecasts_%s.csv", st$station_id))
  tz          <- st$timezone
  now_local   <- now(tzone = tz)
  tomorrow    <- today(tzone = tz) + days(1)

  # ── Skip if past 10 PM local time — forecast for tomorrow is essentially set ──
  if (hour(now_local) >= 22) {
    message(sprintf("  Skipping: %s local time is past 10 PM cutoff.",
                    format(now_local, "%H:%M %Z")))
    next
  }

  issued_local <- format(now_local, "%Y-%m-%d %H:%M %Z")

  # ── Get NWS grid ──
  grid <- tryCatch(get_nws_grid(st$lat, st$lon), error = function(e) {
    message("  Grid lookup failed: ", e$message); NULL
  })
  if (is.null(grid)) next

  # ── Get forecast ──
  fc <- tryCatch(get_forecast_for_date(grid, tomorrow, tz), error = function(e) {
    message("  Forecast failed: ", e$message); NULL
  })
  if (is.null(fc)) {
    message("  No forecast data for ", tomorrow)
    next
  }

  new_row <- data.frame(
    station_id           = as.character(st$station_id),
    station_name         = as.character(st$name),
    forecast_issued_utc  = as.character(issued_utc),
    forecast_issued_local= as.character(issued_local),
    forecast_for_date    = as.character(tomorrow),
    forecast_high_f      = as.character(fc$forecast_high_f),
    n_hours_in_forecast  = as.character(fc$n_hours_in_forecast),
    stringsAsFactors     = FALSE
  )

  # ── Append to CSV (deduplicate by local hour + date) ──
  if (file.exists(outfile)) {
    existing <- read_csv(outfile, col_types = cols(.default = "c"), show_col_types = FALSE)

    # Add missing column if existing CSV predates this change
    if (!"forecast_issued_local" %in% names(existing)) {
      existing$forecast_issued_local <- NA_character_
    }

    # Key: same forecast_for_date + same local hour
    issued_local_hour <- substr(issued_local, 1, 13)
    already_have <- any(
      !is.na(existing$forecast_issued_local) &
      existing$forecast_for_date == new_row$forecast_for_date &
      substr(existing$forecast_issued_local, 1, 13) == issued_local_hour
    )

    if (already_have) {
      message(sprintf("  Already have forecast for %s at this hour. Skipping.", tomorrow))
      next
    }

    combined <- bind_rows(existing, new_row)
  } else {
    combined <- new_row
  }

  write_csv(combined, outfile)
  message(sprintf("  Appended: %s°F forecast for %s (%s hours coverage)",
                  new_row$forecast_high_f, tomorrow, new_row$n_hours_in_forecast))
}

message("\nDone.")
