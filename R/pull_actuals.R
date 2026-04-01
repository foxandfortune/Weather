## ============================================================
## pull_actuals.R
## Pulls official daily high temps (TMAX) from NCEI CDO
## for all stations in stations.csv.
##
## Usage:
##   Rscript pull_actuals.R [start_date] [end_date]
##   Rscript pull_actuals.R 2025-01-01 2025-12-31
##   Rscript pull_actuals.R          # defaults: 2025-01-01 to yesterday
##
## Output: data/actuals_<STATION>.csv per station
## ============================================================

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(dplyr)
  library(lubridate)
  library(readr)
})

# ── Config ────────────────────────────────────────────────────────────────────

NCEI_TOKEN   <- Sys.getenv("NCEI_TOKEN", unset = "PsSEGcmZgsyIySclcwuLzUkXJjcRhJOq")
STATIONS_FILE <- "stations.csv"
DATA_DIR      <- "data"

args       <- commandArgs(trailingOnly = TRUE)
START_DATE <- if (length(args) >= 1) as.Date(args[1]) else as.Date("2025-01-01")
END_DATE   <- if (length(args) >= 2) as.Date(args[2]) else today() - days(1)

dir.create(DATA_DIR, showWarnings = FALSE)

# ── NCEI CDO fetch (handles pagination for long date ranges) ──────────────────
fetch_ncei_tmax <- function(ghcnd_id, start_date, end_date, token) {
  all_results <- list()
  offset      <- 1
  page_size   <- 1000
  base_url    <- "https://www.ncei.noaa.gov/cdo-web/api/v2/data"

  # NCEI CDO caps requests at 1 year; chunk if needed
  date_chunks <- seq(start_date, end_date, by = "year")
  chunk_ends  <- c(date_chunks[-1] - days(1), end_date)

  for (i in seq_along(date_chunks)) {
    chunk_start <- date_chunks[i]
    chunk_end   <- chunk_ends[i]

    message(sprintf("  Fetching %s: %s to %s", ghcnd_id, chunk_start, chunk_end))

    resp <- GET(
      base_url,
      query = list(
        datasetid  = "GHCND",
        stationid  = paste0("GHCND:", ghcnd_id),
        datatypeid = "TMAX",
        startdate  = format(chunk_start, "%Y-%m-%d"),
        enddate    = format(chunk_end,   "%Y-%m-%d"),
        units      = "standard",   # degrees F
        limit      = page_size,
        offset     = offset
      ),
      add_headers(token = token),
      timeout(30)
    )

    if (status_code(resp) == 503) {
      Sys.sleep(5)
      resp <- GET(
        base_url,
        query = list(
          datasetid  = "GHCND",
          stationid  = paste0("GHCND:", ghcnd_id),
          datatypeid = "TMAX",
          startdate  = format(chunk_start, "%Y-%m-%d"),
          enddate    = format(chunk_end,   "%Y-%m-%d"),
          units      = "standard",
          limit      = page_size,
          offset     = offset
        ),
        add_headers(token = token),
        timeout(30)
      )
    }

    if (status_code(resp) != 200) {
      warning(sprintf("NCEI returned %d for %s (%s to %s)",
                      status_code(resp), ghcnd_id, chunk_start, chunk_end))
      next
    }

    body <- fromJSON(rawToChar(content(resp, as = "raw")))

    if (is.null(body$results) || nrow(body$results) == 0) {
      message("  No data returned for this chunk.")
      next
    }

    all_results[[i]] <- body$results %>%
      transmute(
        date            = as.character(as_date(date)),
        observed_high_f = as.character(as.numeric(value))
      )
  }

  bind_rows(all_results)
}

# ── Process each station ──────────────────────────────────────────────────────
stations <- read_csv(STATIONS_FILE, show_col_types = FALSE)

for (i in seq_len(nrow(stations))) {
  st <- stations[i, ]
  message(sprintf("\n=== %s (%s) ===", st$name, st$station_id))

  outfile <- file.path(DATA_DIR, sprintf("actuals_%s.csv", st$station_id))

  # Load existing data to find what's already present
  if (file.exists(outfile)) {
    existing       <- read_csv(outfile, col_types = cols(.default = "c"), show_col_types = FALSE)
    existing_dates <- as.Date(existing$date)
    # Only fetch dates not already in file
    fetch_start <- max(max(existing_dates) + days(1), START_DATE)
    message(sprintf("  Existing data through %s; fetching from %s",
                    max(existing_dates), fetch_start))
  } else {
    existing    <- NULL
    fetch_start <- START_DATE
  }

  if (fetch_start > END_DATE) {
    message("  Already up to date.")
    next
  }

  new_data <- tryCatch(
    fetch_ncei_tmax(st$ncei_ghcnd_id, fetch_start, END_DATE, NCEI_TOKEN),
    error = function(e) { message("  ERROR: ", e$message); NULL }
  )

  if (is.null(new_data) || nrow(new_data) == 0) {
    message("  No new data retrieved.")
    next
  }

  combined <- bind_rows(existing, new_data) %>%
    distinct(date, .keep_all = TRUE) %>%
    arrange(date) %>%
    mutate(station_id = st$station_id, station_name = st$name) %>%
    select(station_id, station_name, date, observed_high_f)

  write_csv(combined, outfile)
  message(sprintf("  Saved %d rows to %s", nrow(combined), outfile))
}

message("\nDone.")
