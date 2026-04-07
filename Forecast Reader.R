library(tidyverse)

read.csv('data/forecasts_KDFW.csv')


forecast_files <- paste0("data/",
                         list.files(path = "data",
                                    pattern = "forecasts_"))

all_forecasts <- purrr::map_df(forecast_files, read.csv) %>% 
  mutate(forecast_for_date = as.Date(forecast_for_date),
         forecast_high_f = as.numeric(forecast_high_f),
         n_hours_in_forecast = as.numeric(n_hours_in_forecast))

kalshi_results <- read.csv('data/kalshi_results.csv') %>% 
  mutate(market_date = as.Date(market_date))


str(all_forecasts)
str(kalshi_results)

all_forecasts %>% 
  filter(forecast_for_date == Sys.Date()) %>% 
  with_groups(.groups = station_id,
              slice,
              n()) %>% 
  select(station_id,
         station_name,
         forecast_for_date,
         forecast_high_f)

unique(all_forecasts$forecast_for_date)


all_forecasts %>% 
  with_groups(.groups = c(station_id, forecast_for_date),
              slice,
              n()) %>% 
  select(station_id,
         station_name,
         date = forecast_for_date,
         forecast_high_f) %>% 
  left_join(kalshi_results %>% 
              mutate(yes_sub_title = str_remove_all(yes_sub_title, "°")) %>% 
              mutate(temp_range = case_when(
                str_detect(yes_sub_title, " or below") ~ as.numeric(str_sub(yes_sub_title, 1, 2)),
                str_detect(yes_sub_title, " or above") ~ as.numeric(str_sub(yes_sub_title, 1, 2)),
                TRUE ~ (as.numeric(str_sub(yes_sub_title, 1, 2)) + as.numeric(str_sub(yes_sub_title, -2, -1))) / 2),
                type = case_when(
                  str_detect(yes_sub_title, " or below") ~ "below",
                  str_detect(yes_sub_title, " or above") ~ "above",
                  TRUE ~ "range"
                )) %>% 
              select(station_id,
                     date = market_date,
                     temp_range,
                     type),
            by = c("date", "station_id"))%>% 
  filter(!is.na(temp_range),
         !is.na(forecast_high_f)) %>% 
  with_groups(.groups = station_id,
              summarise,
              rmse = Metrics::rmse(forecast_high_f,
                                   temp_range))

kalshi_results %>% 
  mutate(yes_sub_title = str_remove_all(yes_sub_title, "°")) %>% 
  mutate(temp_range = case_when(
    str_detect(yes_sub_title, " or below") ~ as.numeric(str_sub(yes_sub_title, 1, 2)),
    str_detect(yes_sub_title, " or above") ~ as.numeric(str_sub(yes_sub_title, 1, 2)),
    TRUE ~ (as.numeric(str_sub(yes_sub_title, 1, 2)) + as.numeric(str_sub(yes_sub_title, -2, -1))) / 2),
    type = case_when(
      str_detect(yes_sub_title, " or below") ~ "below",
      str_detect(yes_sub_title, " or above") ~ "above",
      TRUE ~ "range"
    )) %>% 
  select(station_id,
         date = market_date,
         temp_range,
         type) 
