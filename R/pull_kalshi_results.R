## ============================================================
## pull_kalshi_results.R
## Pulls settled Kalshi high temperature market results
## for all stations in stations.csv.
##
## Strategy:
##   - For each station/series, fetch the event for each date
##   - Each event contains multiple markets (one per temp range)
##   - Find the settled "yes" market to get the actual outcome
##
## No API key required — market data is public.
##
## Usage:
##   Rscript pull_kalshi_results.R [start_date] [end_date]
##   Rscript pull_kalshi_results.R 2026-04-03 2026-04-05
##   Rscript pull_kalshi_results.R          # defaults: yesterday only
##
## Output: data/kalshi_results.csv
## ============================================================

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(dplyr)
  library(lubridate)
  library(readr)
  library(purrr)
})

# ── Config ────────────────────────────────────────────────────────────────────

STATIONS_FILE <- "stations.csv"
DATA_DIR      <- "data"
OUTPUT_FILE   <- file.path(DATA_DIR, "kalshi_results.csv")
BASE_URL      <- "https://api.elections.kalshi.com/trade-api/v2"

args       <- commandArgs(trailingOnly = TRUE)
START_DATE <- if (length(args) >= 1) as.Date(args[1]) else today() - days(1)
END_DATE   <- if (length(args) >= 2) as.Date(args[2]) else today() - days(1)

dir.create(DATA_DIR, showWarnings = FALSE)

# ── Helpers ───────────────────────────────────────────────────────────────────

kalshi_get <- function(path, query = list()) {
  url <- paste0(BASE_URL, path)
  resp <- GET(url, query = query,
              add_headers("User-Agent" = "KalshiWeatherTracker/1.0"),
              timeout(20))
  if (status_code(resp) != 200) {
    warning(sprintf("HTTP %d for %s", status_code(resp), url))
    return(NULL)
  }
  fromJSON(rawToChar(content(resp, as = "raw")), flatten = TRUE)
}

# Format date as Kalshi event ticker suffix: 2026-04-03 -> "26APR03"
date_to_kalshi <- function(d) {
  toupper(format(as.Date(d), "%y%b%d"))
}

# ── Fetch one event's markets and find the settled outcome ────────────────────
fetch_event_result <- function(series_ticker, date) {
  event_ticker <- paste0(series_ticker, "-", date_to_kalshi(date))

  # Try live markets endpoint first (recent dates)
  resp <- kalshi_get(paste0("/events/", event_ticker))

  # Fall back to historical if not found
  if (is.null(resp)) {
    resp <- kalshi_get(paste0("/historical/events/", event_ticker))
  }

  if (is.null(resp) || is.null(resp$event)) {
    message(sprintf("    No event found: %s", event_ticker))
    return(NULL)
  }

  event <- resp$event
  markets <- resp$markets

  if (is.null(markets) || length(markets) == 0) {
    message(sprintf("    No markets in event: %s", event_ticker))
    return(NULL)
  }

  # markets is a data frame — find the one settled as "yes"
  if (!"result" %in% names(markets)) {
    message(sprintf("    No result field in markets for: %s", event_ticker))
    return(NULL)
  }

  settled_yes <- markets %>% filter(result == "yes")
  settled_any <- markets %>% filter(status %in% c("settled", "determined"))

  if (nrow(settled_yes) == 0) {
    if (nrow(settled_any) == 0) {
      message(sprintf("    Not yet settled: %s", event_ticker))
    } else {
      message(sprintf("    Settled but no YES result found: %s", event_ticker))
    }
    return(NULL)
  }

  # The yes_sub_title describes the winning range, e.g. "62 to 63" or "Above 90"
  winning_market <- settled_yes[1, ]

  data.frame(
    series_ticker       = series_ticker,
    event_ticker        = event_ticker,
    market_date         = as.character(date),
    winning_ticker      = winning_market$ticker,
    yes_sub_title       = winning_market$yes_sub_title %||% NA_character_,
    no_sub_title        = winning_market$no_sub_title  %||% NA_character_,
    result              = winning_market$result,
    close_time          = as.character(winning_market$close_time %||% NA_character_),
    volume_fp           = as.character(winning_market$volume_fp  %||% NA_character_),
    stringsAsFactors    = FALSE
  )
}

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0 && !is.na(a)) a else b

# ── Main loop ─────────────────────────────────────────────────────────────────

stations <- read_csv(STATIONS_FILE, show_col_types = FALSE)
dates    <- seq(START_DATE, END_DATE, by = "day")

message(sprintf("Pulling Kalshi results for %d stations, %s to %s",
                nrow(stations), START_DATE, END_DATE))

all_results <- list()

for (i in seq_len(nrow(stations))) {
  st <- stations[i, ]
  message(sprintf("\n--- %s (%s) ---", st$name, st$kalshi_series))

  for (d in as.character(dates)) {
    message(sprintf("  %s", d))
    result <- tryCatch(
      fetch_event_result(st$kalshi_series, as.Date(d)),
      error = function(e) { message("  ERROR: ", e$message); NULL }
    )

    if (!is.null(result)) {
      result$station_id   <- st$station_id
      result$station_name <- st$name
      all_results[[length(all_results) + 1]] <- result
      message(sprintf("    Settled YES: %s", result$yes_sub_title))
    }

    Sys.sleep(0.2)  # be polite to the API
  }
}

if (length(all_results) == 0) {
  message("\nNo results found. Markets may not be settled yet or tickers may have changed.")
  quit(status = 0)
}

new_data <- bind_rows(all_results) %>%
  select(station_id, station_name, series_ticker, event_ticker,
         market_date, winning_ticker, yes_sub_title, no_sub_title,
         result, close_time, volume_fp)

# ── Merge with existing file ──────────────────────────────────────────────────

if (file.exists(OUTPUT_FILE)) {
  existing <- read_csv(OUTPUT_FILE, col_types = cols(.default = "c"),
                       show_col_types = FALSE)
  combined <- bind_rows(existing, new_data %>% mutate(across(everything(), as.character))) %>%
    distinct(event_ticker, winning_ticker, .keep_all = TRUE) %>%
    arrange(market_date, station_id)
} else {
  combined <- new_data %>%
    mutate(across(everything(), as.character)) %>%
    arrange(market_date, station_id)
}

write_csv(combined, OUTPUT_FILE)
message(sprintf("\nSaved %d rows to %s", nrow(combined), OUTPUT_FILE))
print(combined %>% select(station_id, market_date, yes_sub_title, volume_fp))
