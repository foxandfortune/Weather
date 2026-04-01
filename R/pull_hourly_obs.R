## ============================================================
## pull_hourly_obs.R
## Pulls hourly ASOS observations for KHOU from the
## Iowa Environmental Mesonet (IEM) archive.
##
## IEM is free, no token needed, returns clean CSV, and has
## data going back decades. Much more reliable than NCEI APIs.
##
## Usage: Rscript pull_hourly_obs.R
## Output: hourly_obs_KHOU.csv
## ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(lubridate)
  library(readr)
})

# ── Config ────────────────────────────────────────────────────────────────────

STATION_ID  <- "KHOU"
START_DATE  <- as.Date("2025-01-01")
END_DATE    <- today() - days(1)
OUTPUT_FILE <- "hourly_obs_KHOU.csv"
TZ          <- "America/Chicago"

c_to_f <- function(c) round(as.numeric(c) * 9/5 + 32, 1)

# ── Build IEM URL ─────────────────────────────────────────────────────────────
# IEM ASOS request docs: https://mesonet.agron.iastate.edu/request/download.phtml
# tmpf = temp in Fahrenheit, dwpf = dewpoint, relh = humidity, sknt = wind knots
# tz param returns timestamps in local time
# report_type=3 = routine hourly METARs only (excludes specials)

build_iem_url <- function(station, start, end, tz = "America/Chicago") {
  paste0(
    "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py",
    "?station=", station,
    "&data=tmpf&data=dwpf&data=relh&data=sknt&data=drct&data=p01i",
    "&year1=",  year(start),  "&month1=", month(start), "&day1=", day(start),
    "&year2=",  year(end),    "&month2=", month(end),   "&day2=", day(end),
    "&tz=",     URLencode(tz, reserved = TRUE),
    "&format=onlycomma",
    "&latlon=no",
    "&missing=M",
    "&trace=T",
    "&direct=no",
    "&report_type=3"   # routine hourly only
  )
}

# ── Download ──────────────────────────────────────────────────────────────────
# IEM handles the full date range in one request — no chunking needed

url <- build_iem_url(STATION_ID, START_DATE, END_DATE)
message("Downloading from IEM...")
message("URL: ", url)

tmp <- tempfile(fileext = ".csv")
result <- tryCatch(
  download.file(url, destfile = tmp, quiet = FALSE, method = "libcurl"),
  error = function(e) { message("Download error: ", e$message); -1 }
)

if (result != 0 || !file.exists(tmp) || file.size(tmp) < 100) {
  stop("Download failed. Check URL above in a browser to verify IEM is reachable.")
}

# ── Parse ─────────────────────────────────────────────────────────────────────
# IEM returns a header comment block starting with #, then the CSV

raw_lines <- readLines(tmp, warn = FALSE)
message(sprintf("Raw lines: %d", length(raw_lines)))

# Show first few lines for debugging
message("First 5 lines:")
cat(paste(head(raw_lines, 5), collapse = "\n"), "\n")

# Strip comment lines (start with #)
data_lines <- raw_lines[!grepl("^#", raw_lines)]

if (length(data_lines) <= 1) {
  stop("No data rows found. Check the URL in a browser.")
}

df_raw <- read_csv(
  paste(data_lines, collapse = "\n"),
  show_col_types = FALSE,
  col_types = cols(.default = "c")
)

message(sprintf("Parsed %d rows, columns: %s",
                nrow(df_raw), paste(names(df_raw), collapse = ", ")))

# ── Clean and convert ─────────────────────────────────────────────────────────
# IEM timestamps are in the requested timezone (America/Chicago)
# tmpf is already in Fahrenheit

df <- df_raw %>%
  filter(!is.na(valid), tmpf != "M", tmpf != "") %>%
  mutate(
    obs_local  = ymd_hm(valid, tz = TZ, quiet = TRUE),
    obs_utc    = with_tz(obs_local, "UTC"),
    date_local = as_date(obs_local),
    hour_local = hour(obs_local),
    temp_f     = round(as.numeric(tmpf), 1),
    temp_c     = round((temp_f - 32) * 5/9, 1),
    dewpoint_f = suppressWarnings(as.numeric(dwpf)),
    rh_pct     = suppressWarnings(as.numeric(relh)),
    wind_kts   = suppressWarnings(as.numeric(sknt)),
    wind_dir   = suppressWarnings(as.numeric(drct)),
    precip_in  = suppressWarnings(as.numeric(p01i))
  ) %>%
  filter(!is.na(obs_local), !is.na(temp_f)) %>%
  arrange(obs_utc) %>%
  mutate(station_id = STATION_ID) %>%
  select(station_id, obs_utc, obs_local, date_local, hour_local,
         temp_f, temp_c, dewpoint_f, rh_pct, wind_kts, wind_dir, precip_in)

if (nrow(df) == 0) stop("Zero rows after filtering.")

write_csv(df, OUTPUT_FILE)

message(sprintf("\nSaved %d hourly rows to %s", nrow(df), OUTPUT_FILE))
message(sprintf("Date range:  %s to %s", min(df$date_local), max(df$date_local)))
message(sprintf("Temp range:  %.1f F to %.1f F",
                min(df$temp_f, na.rm = TRUE), max(df$temp_f, na.rm = TRUE)))
message("\nSample rows:")
print(head(df, 6))

saveRDS(df, 'data/khou_hourly_historic.rds')
