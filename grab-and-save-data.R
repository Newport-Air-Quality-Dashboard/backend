#!/usr/bin/env Rscript
# install.packages(c("httr", "jsonlite", "tidyverse"))

library(con2aqi)
library(httr)
library(jsonlite)
library(tidyverse)

# I decided it was a bad idea to have api keys in a repo so i've used "config.R"
# to store sensitive data. just copy the config.R.example to config.R.
source("config.R")

# Function to pull data from the EPA's AirNow EPA. Requires an AirNow account
# and an API key. 
get_epa <- function(nwlng, nwlat,  selng, selat,
                    data_type = "B", monitor_type = "2", verbose = "1",
                    parameters = c("OZONE", "PM25", "PM10", "CO", "NO2", "SO2"),
                    include_raw_concentrations = "1", api_key,
                    start_date, end_date) {

  # epa allows you to select an area for sensors. this creates that from 2 lats
  # and 2 longs
  bbox <- paste(nwlng, nwlat, selng, selat, sep=",")

  root_url <- "https://www.airnowapi.org/aq/data/?"

  parameter_url <- paste0("format=", "text/csv",
                         "&BBOX=", bbox, 
                         "&parameters=", paste(parameters, collapse=","),
                         "&dataType=", data_type,
                         "&monitorType=", monitor_type,
                         "&verbose=", verbose,
                         "&includerawconcentrations=", include_raw_concentrations,
                         "&API_KEY=", api_key)

  # if you specify start_date and end_date, it'll pull data for that entire
  # period. by default it will pull the most recent hour.
  if(!missing(start_date) && !missing(end_date)) {
    parameter_url <- paste0(parameter_url, 
                            "&startDate=", start_date,
                            "&endDate=", end_date)
  }
  
  # build the final url to request from
  api_url <- paste0(root_url, parameter_url)
  
  response <- GET(api_url)
  
  if (status_code(response) == 200) {
    df <- content(response, as = "parsed")
    
    colnames(df) <- c("latitude", "longitude", "time_stamp", "parameter", 
                          "concentration", "unit", "raw_concentration", "aqi", 
                          "category", "name", "agency", "aqs_id", "sensor_index")
    
  } else {
    stop("API request failed")
  }
  return (df)
}

# Transforms the EPA data from long to wide format and renames the columns to 
# be synonymous with PurpleAir's data. 
transform_epa <- function(df) {
  
  # aqs_id is redundant. there's another field called "full aqs id" that
  # contains the aqs_id and also the country code
  df$aqs_id <- NULL 
  
  # epa uses -999 to signify empty values. I've also seen -999.0 so replace
  # both to be safe.
  df[df == -999] <- NA
  df[df == -999.0] <- NA
    
  # converts the epa data from long to wide form.
  df <- df %>% pivot_wider(names_from=parameter, values_from=c(concentration, aqi, category, raw_concentration, unit)) 
  
  # vector used to rename the epa fields to match purple air.
  to_rename <- c(pm2.5_aqi = "aqi_PM2.5",
                 no2_aqi = "aqi_NO2",
                 co_aqi = "aqi_CO",
                 ozone_aqi = "aqi_OZONE",
                 pm10.0_aqi = "aqi_PM10",
                 so2_aqi = "aqi_SO2",
                 pm2.5_category = "category_PM2.5", 
                 no2_category = "category_NO2",
                 co_category = "category_CO",
                 ozone_category = "category_OZONE",
                 pm10.0_category = "category_PM10",
                 so2_category = "category_SO2",
                 pm2.5_unit = "unit_PM2.5", 
                 no2_unit = "unit_NO2",
                 co_unit = "unit_CO",
                 ozone_unit = "unit_OZONE",
                 pm10.0_unit = "unit_PM10",
                 so2_unit = "unit_SO2",
                 pm2.5_60minute = "concentration_PM2.5", 
                 no2_60minute = "concentration_NO2",
                 co_60minute = "concentration_CO",
                 ozone_60minute = "concentration_OZONE",
                 pm10.0_60minute = "concentration_PM10",
                 so2_60minute = "concentration_SO2",
                 pm2.5_atm = "raw_concentration_PM2.5", 
                 no2_atm = "raw_concentration_NO2",
                 co_atm = "raw_concentration_CO",
                 ozone_atm = "raw_concentration_OZONE",
                 pm10.0_atm = "raw_concentration_PM10",
                 so2_atm = "raw_concentration_SO2")
  
  # originally, i had issues with the program erroring and crashing if a field
  # was absent that was trying to be renamed. using any_of and a character
  # vector fixed that.
  df <- df %>% rename(any_of(to_rename))
  
  # no2, co, and so2 have the same values in "concentration so i remove these 
  # fields to prevent misunderstandings of the data.
  df[, c('no2_60minute', 'co_60minute', 'so2_60minute')] <- list(NULL)
  
  # time_stamp is datetime
  df$time_stamp <- as.POSIXct(df$time_stamp,
                              format = "%Y-%m-%dT%H:%M", tz="UTC") 
  
  # probably doesn't matter, but makes sure the sensor_index displays as it should
  df$sensor_index <- as.character(df$sensor_index)
  
  df$source <- "EPA"
  
  return(df)
}


# grabs pa data from the purpleair api. requires an api key with points.
get_pa <- function(nwlng, nwlat, selng, selat, location, api_key) {
  # PurpleAir API URL
  root_url <- "https://api.purpleair.com/v1/sensors/"
  
  # Box domain: lat_lon = [nwlng,, nwlat, selng, selat]
  lat_lon <- c(nwlng, nwlat, selng, selat)
  ll_api_url <- paste0("&nwlng=", lat_lon[1], "&nwlat=", lat_lon[2], "&selng=", lat_lon[3], "&selat=", lat_lon[4])
  
  # Fields to get
  fields_list <- c("sensor_index", 
                   "last_seen", 
                   "name", 
                   "latitude", 
                   "longitude", 
                   "humidity_a", "humidity_b", 
                   "temperature_a", "temperature_b",
                   "pressure_a", "pressure_b",
                   "pm1.0_a", "pm1.0_b",
                   "pm2.5_atm_a", "pm2.5_atm_b",
                   "pm2.5_10minute_a", "pm2.5_10minute_b",
                   "pm10.0_atm_a", "pm10.0_atm_b",
                   "confidence")
  
  fields_api_url <- paste0("&fields=", paste(fields_list, collapse = "%2C"))
  
  # Final API URL
  api_url <- paste0(root_url, "?api_key=", api_key, fields_api_url, ll_api_url)
  
  # Getting data
  response <- GET(api_url)
  
  if (status_code(response) == 200) {
    json_data <- content(response, as = "text") %>%
      fromJSON()
    df <- bind_cols(json_data$data)
    names(df) <- fields_list
  } else {
    stop("API request failed")
  }
  
  return(df)
}

transform_pa_historical <- function(df) {
  # null is how PA signifies missing data
  df[df == "null"] <- NA
  
  df$time_stamp <- as.POSIXct(df$time_stamp, 
                              format = "%s",
                              tz = "UTC") 
  df$sensor_index <- as.character(df$sensor_index)
  
  # this prevents errors when trying to join with epa data later
  df <- df %>% mutate_at(c('latitude', 
                           'longitude',
                           'humidity_a', 'humidity_b',
                           'temperature_a', 'temperature_b',
                           'pressure_a', 'pressure_b',
                           'pm1.0_60minute_a', 'pm1.0_60minute_b',
                           'pm2.5_60minute_a', 'pm2.5_60minute_b',
                           'pm10.0_60minute_a', 'pm10.0_60minute_b'), as.numeric)  
  
  # grabbing _a and _b channels is the same cost as grabbing the averaged so this
  # gives us more data and also a way to test confidence of data.
  df$humidity <- rowMeans(df[, c('humidity_a', 'humidity_b')])
  df$temperature <- rowMeans(df[, c('temperature_a', 'temperature_b')])
  df$pressure <- rowMeans(df[, c('pressure_a', 'pressure_b')])
  df$pm1.0 <- rowMeans(df[, c('pm1.0_60minute_a', 'pm1.0_60minute_b')])
  df$pm2.5_60minute <- rowMeans(df[, c('pm2.5_60minute_a', 'pm2.5_60minute_b')])
  df$pm10.0_60minute <- rowMeans(df[, c('pm10.0_60minute_a', 'pm10.0_60minute_b')])
  
  # calculate aqi for pm2.5 and pm10.0
  df$pm2.5_aqi[!is.na(df$pm2.5_60minute) & df$pm2.5_60minute <= 500.4] <- con2aqi("pm25", df$pm2.5_60minute[!is.na(df$pm2.5_60minute) & df$pm2.5_60minute <= 500.4])
  df$pm10.0_aqi[!is.na(df$pm10.0_60minute) & df$pm10.0_60minute <= 604] <- con2aqi("pm10", df$pm10.0_60minute[!is.na(df$pm10.0_60minute) & df$pm10.0_60minute <= 604])
  
  df$source <- "PurpleAir"
  
  return(df)
}

transform_pa <- function(df) {
  
  # last_seen reports the time and data associated with that time
  df <- df %>% rename(any_of(c(time_stamp = "last_seen")))
  
  # null is how PA signifies missing data
  df[df == "null"] <- NA
  
  df$time_stamp <- as.POSIXct(df$time_stamp, 
                              format = "%s",
                              tz = "UTC") 
  df$sensor_index <- as.character(df$sensor_index)
  
  # this prevents errors when trying to join with epa data later
  df <- df %>% mutate_at(c('latitude', 
                           'longitude',
                           'humidity_a', 'humidity_b',
                           'temperature_a', 'temperature_b',
                           'pressure_a', 'pressure_b',
                           'pm1.0_atm_a', 'pm1.0_atm_b',
                           'pm2.5_atm_a', 'pm2.5_atm_b',
                           'pm2.5_10minute_a', 'pm2.5_10minute_b',
                           'pm10.0_atm_a', 'pm10.0_atm_b',
                           'confidence'), as.numeric)  
  
  # grabbing _a and _b channels is the same cost as grabbing the averaged so this
  # gives us more data and also a way to test confidence of data.
  df$humidity <- rowMeans(df[, c('humidity_a', 'humidity_b')])
  df$temperature <- rowMeans(df[, c('temperature_a', 'temperature_b')])
  df$pressure <- rowMeans(df[, c('pressure_a', 'pressure_b')])
  df$pm1.0 <- rowMeans(df[, c('pm1.0_a', 'pm1.0_b')])
  df$pm2.5_atm <- rowMeans(df[, c('pm2.5_atm_a', 'pm2.5_atm_b')])
  df$pm2.5_10minute <- rowMeans(df[, c('pm2.5_10minute_a', 'pm2.5_10minute_b')])
  df$pm10.0_atm <- rowMeans(df[, c('pm10.0_atm_a', 'pm10.0_atm_b')])
  
  # calculate aqi for pm2.5 and pm10.0
  df$pm2.5_aqi[df$pm2.5_10minute <= 500.4] <- con2aqi("pm25", df$pm2.5_10minute[df$pm2.5_10minute <= 500.4])
  df$pm10.0_aqi[df$pm10.0_atm <= 604] <- con2aqi("pm10", df$pm10.0_atm[df$pm10.0_atm <= 604])
  
  df$source <- "PurpleAir"
  
  return(df)
}

nwlng <- -85.825195 
nwlat <- 38.324420 
selng <- -83.023682
selat <- 39.825413

input_df="./out/df_all.Rda"
output_df="./out/df_all.Rda"
location <- "both"

load(input_df)

epa_time <- 1
while (T) {
  print("grabbing PurpleAir data")
  df_pa <- get_pa(-84.534, 39.106, -84.455, 39.050, location, api_key=pa_key)
  print("transforming PurpleAir data")
  df_pa <- transform_pa(df_pa)
  
  if (epa_time %% 6 == 0) {
    print("grabbing EPA data")
    df_epa <- get_epa(nwlng = nwlng, nwlat = nwlat, selng = selng, selat = selat,
                             api_key = epa_key)
    print("transforming EPA data")
    df_epa <- transform_epa(df_epa) 
    
    print("joining PurpleAir and EPA data")
    df_new <- list(df_pa, df_epa) %>% reduce(full_join, by=c('sensor_index', 
                                                             'time_stamp', 
                                                             'latitude', 
                                                             'longitude', 
                                                             'name', 
                                                             'source'))
    epa_time <- 1
  } else {
    df_new <- df_pa
  }
  
  df_new$type <- "real-time"
  
  print("merging real-time with historical data")
  combined_data <- bind_rows(combined_data, df_new)
  
  print("removing duplicate rows")
  combined_data <- combined_data %>% distinct()
  
  print("saving data")
  save(combined_data, file=output_df)

  print("data saved. sleeping 10 minutes")
  
  epa_time <- epa_time + 1
  Sys.sleep(600) # Sleep 10m
}

  # df_pa <- transform_pa_historical(df_pa)
  # 
  # df_epa <- transform_epa(df_epa)
  # df_new <- df_epa
  # 
  # combined_data$pm2.5_aqi[!is.na(combined_data$pm2.5_60minute) & combined_data$pm2.5_60minute <= 500.4 & is.na(combined_data$pm2.5_aqi)] <- con2aqi("pm25", combined_data$pm2.5_60minute[!is.na(combined_data$pm2.5_60minute) & combined_data$pm2.5_60minute <= 500.4 & is.na(combined_data$pm2.5_aqi)])
  # combined_data$pm10.0_aqi[!is.na(combined_data$pm10.0_60minute) & combined_data$pm10.0_60minute <= 604 & is.na(combined_data$pm10.0_aqi)] <- con2aqi("pm10", combined_data$pm10.0_60minute[!is.na(combined_data$pm10.0_60minute) & combined_data$pm10.0_60minute <= 604 & is.na(combined_data$pm10.0_aqi)])
  # save(combined_data, file="out/combined_aqis.Rda")
  # 
  # load(file="out/combined_aqis.Rda")
  # 
  # combined_data$pm2.5_60minute[is.na(combined_data$pm2.5_60minute)] <- rowMeans(combined_data[is.na(combined_data$pm2.5_60minute), c("pm2.5_60minute_a", "pm2.5_60minute_b")])
  # combined_data$pm10.0_60minute[is.na(combined_data$pm10.0_60minute)] <- rowMeans(combined_data[is.na(combined_data$pm10.0_60minute), c("pm10.0_60minute_a", "pm10.0_60minute_b")])
  # combined_data$pm1.0_60minute[is.na(combined_data$pm1.0_60minute)] <- rowMeans(combined_data[is.na(combined_data$pm1.0_60minute), c("pm1.0_60minute_a", "pm1.0_60minute_b")])
  # combined_data$temperature[is.na(combined_data$temperature)] <- rowMeans(combined_data[is.na(combined_data$temperature), c("temperature_a", "temperature_b")])
  # combined_data$humidity[is.na(combined_data$humidity)] <- rowMeans(combined_data[is.na(combined_data$humidity), c("humidity_a", "humidity_b")])
  # combined_data$pressure[is.na(combined_data$pressure)] <- rowMeans(combined_data[is.na(combined_data$pressure), c("pressure_a", "pressure_b")])
  # 
  # df_all <- combined_data
  # 
  # save(df_all, file="out/df_all.Rda")
  # load(file="out/df_all.Rda")
  
