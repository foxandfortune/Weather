## ============================================================
## pull_timeseries.R
## Pulls sub-hourly temperature observations for all stations
## in stations.csv using the IEM ASOS archive — the same data
## source that powers weather.gov/wrh/timeseries.
##
## For historical dates: IEM ASOS (complete, reliable)
## For today (live):     NWS station observations API
##
## Usage:
##   Rscript pull_timeseries.R [start_date] [end_date]
##   Rscript pull_timeseries.R 2026-04-03 2026-04-05
##   Rscript pull_timeseries.R          # defaults: yesterday
##
## Output: data/timeseries_<STATION>.csv per station
## ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(lubridate)
  library(readr)
  library(httr)
  library(jsonlite)
})

# ── Config ────────────────────────────────────────────────────────────────────

STATIONS_FILE <- "stations.csv"
DATA_DIR      <- "data"

args       <- commandArgs(trailingOnly = TRUE)
START_DATE <- if (length(args) >= 1) as.Date(args[1]) else today() - days(0)
END_DATE   <- if (length(args) >= 2) as.Date(args[2]) else today() - days(0)

dir.create(DATA_DIR, showWarnings = FALSE)

c_to_f <- function(c) round(as.numeric(c) * 9/5 + 32, 1)

# ── IEM ASOS: historical + recent data ────────────────────────────────────────
# Returns all METAR obs (routine + specials) with temp, dewpoint, wind, precip
# Same data source as weather.gov/wrh/timeseries

fetch_iem <- function(station_id, start_date, end_date, tz) {
  url <- paste0(
    "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py",
    "?station=", station_id,
    "&data=tmpf&data=dwpf&data=relh&data=sknt&data=drct&data=p01i&data=feel",
    "&year1=",  year(start_date),  "&month1=", month(start_date), "&day1=", day(start_date),
    "&year2=",  year(end_date),    "&month2=", month(end_date),   "&day2=", day(end_date),
    "&tz=",     URLencode(tz, reserved = TRUE),
    "&format=onlycomma",
    "&latlon=no",
    "&missing=M",
    "&trace=T",
    "&direct=no",
    "&report_type=3&report_type=1"  # routine hourly + specials
  )

  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp), add = TRUE)

  result <- tryCatch(
    suppressWarnings(download.file(url, destfile = tmp, quiet = TRUE, method = "libcurl")),
    error = function(e) { message("  IEM download error: ", e$message); -1 }
  )

  if (result != 0 || !file.exists(tmp) || file.size(tmp) < 50) return(NULL)

  raw <- readLines(tmp, warn = FALSE)
  data_lines <- raw[!grepl("^#", raw)]
  if (length(data_lines) <= 1) return(NULL)

  df <- read_csv(paste(data_lines, collapse = "\n"),
                 show_col_types = FALSE,
                 col_types = cols(.default = "c"))

  df %>%
    filter(!is.na(valid), tmpf != "M", tmpf != "") %>%
    mutate(
      obs_local  = ymd_hm(valid, tz = tz, quiet = TRUE),
      obs_utc    = with_tz(obs_local, "UTC"),
      date_local = as_date(obs_local),
      hour_local = hour(obs_local),
      temp_f     = round(as.numeric(tmpf), 1),
      temp_c     = round((temp_f - 32) * 5/9, 1),
      dewpoint_f = suppressWarnings(as.numeric(dwpf)),
      rh_pct     = suppressWarnings(as.numeric(relh)),
      wind_kts   = suppressWarnings(as.numeric(sknt)),
      wind_dir   = suppressWarnings(as.numeric(drct)),
      precip_in  = suppressWarnings(as.numeric(p01i)),
      feels_like_f = suppressWarnings(as.numeric(feel)),
      source     = "IEM_ASOS"
    ) %>%
    filter(!is.na(obs_local), !is.na(temp_f)) %>%
    arrange(obs_utc)
}

# ── NWS API: live observations for today ──────────────────────────────────────
fetch_nws_live <- function(station_id, tz) {
  url  <- sprintf("https://api.weather.gov/stations/%s/observations?limit=200", station_id)
  resp <- GET(url,
              add_headers("User-Agent" = "KalshiWeatherTracker/1.0"),
              timeout(20))

  if (status_code(resp) != 200) return(NULL)

  parsed   <- fromJSON(rawToChar(content(resp, as = "raw")), flatten = TRUE)
  features <- parsed$features
  if (is.null(features) || nrow(features) == 0) return(NULL)

  features %>%
    transmute(
      obs_utc      = ymd_hms(properties.timestamp, tz = "UTC"),
      obs_local    = with_tz(obs_utc, tz),
      date_local   = as_date(obs_local),
      hour_local   = hour(obs_local),
      temp_c       = round(as.numeric(properties.temperature.value), 1),
      temp_f       = c_to_f(temp_c),
      dewpoint_f   = c_to_f(as.numeric(properties.dewpoint.value)),
      rh_pct       = round(as.numeric(properties.relativeHumidity.value), 1),
      wind_kts     = round(as.numeric(properties.windSpeed.value) * 0.539957, 1), # m/s to kts
      wind_dir     = as.numeric(properties.windDirection.value),
      precip_in    = NA_real_,
      feels_like_f = NA_real_,
      source       = "NWS_API"
    ) %>%
    filter(!is.na(temp_c)) %>%
    arrange(obs_utc)
}

# ── Main ──────────────────────────────────────────────────────────────────────

stations <- read_csv(STATIONS_FILE, show_col_types = FALSE)
today_date <- today()

message(sprintf("Pulling timeseries for %d stations: %s to %s",
                nrow(stations), START_DATE, END_DATE))

for (i in seq_len(nrow(stations))) {
  st <- stations[i, ]
  message(sprintf("\n--- %s (%s) ---", st$name, st$station_id))

  outfile <- file.path(DATA_DIR, sprintf("timeseries_%s.csv", st$station_id))

  # Split date range: historical (IEM) vs today (NWS live)
  hist_end   <- min(END_DATE, today_date - days(1))
  fetch_live <- END_DATE >= today_date

  all_obs <- list()

  # Historical via IEM
  if (START_DATE <= hist_end) {
    message(sprintf("  IEM: %s to %s", START_DATE, hist_end))
    iem_data <- tryCatch(
      fetch_iem(st$station_id, START_DATE, hist_end, st$timezone),
      error = function(e) { message("  IEM error: ", e$message); NULL }
    )
    if (!is.null(iem_data) && nrow(iem_data) > 0) {
      message(sprintf("  IEM: %d observations", nrow(iem_data)))
      all_obs[[length(all_obs) + 1]] <- iem_data
    }
  }

  # Live/today via NWS API
  if (fetch_live) {
    message("  NWS API: live observations for today")
    nws_data <- tryCatch(
      fetch_nws_live(st$station_id, st$timezone),
      error = function(e) { message("  NWS error: ", e$message); NULL }
    )
    if (!is.null(nws_data) && nrow(nws_data) > 0) {
      today_obs <- nws_data %>% filter(date_local == today_date)
      message(sprintf("  NWS API: %d observations today", nrow(today_obs)))
      all_obs[[length(all_obs) + 1]] <- today_obs
    }
  }

  if (length(all_obs) == 0) {
    message("  No data retrieved.")
    next
  }

  new_data <- bind_rows(all_obs) %>%
    mutate(station_id = st$station_id, station_name = st$name) %>%
    select(station_id, station_name, obs_utc, obs_local, date_local,
           hour_local, temp_f, temp_c, dewpoint_f, rh_pct,
           wind_kts, wind_dir, precip_in, feels_like_f, source)

  # Merge with existing, deduplicate on obs_utc
  if (file.exists(outfile)) {
    existing <- read_csv(outfile, col_types = cols(.default = "c"),
                         show_col_types = FALSE)
    combined <- bind_rows(
      existing,
      new_data %>% mutate(across(everything(), as.character))
    ) %>%
      distinct(station_id, obs_utc, .keep_all = TRUE) %>%
      arrange(obs_utc)
  } else {
    combined <- new_data %>%
      mutate(across(everything(), as.character)) %>%
      arrange(obs_utc)
  }

  write_csv(combined, outfile)
  message(sprintf("  Saved %d total rows to %s", nrow(combined), outfile))
}

message("\nDone.")

all_obs
