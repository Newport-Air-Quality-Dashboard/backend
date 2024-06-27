#!/usr/bin/env Rscript

# Install packages that aren't installed
my_packages <- c("con2aqi", 
                 "httr", 
                 "jsonlite", 
                 "tidyverse",
                 "DBI",
                 "RSQLite")

not_installed <- my_packages[!(my_packages %in% installed.packages()[ , "Package"])]    # Extract not installed packages
if(length(not_installed)) install.packages(not_installed)    

library(con2aqi)
library(httr)
library(jsonlite)
library(tidyverse)

library(DBI)

# I decided it was a bad idea to have api keys in a repo so i've used "config.R"
# to store sensitive data. just copy the config.R.example to config.R.
source("config.R")

# opens sqlite database specified in config.R
con <- dbConnect(RSQLite::SQLite(), path_to_db)



##  _____ ____   _     ##
## | ____|  _ \ / \    ##
## |  _| | |_) / _ \   ##
## | |___|  __/ ___ \  ##
## |_____|_| /_/   \_\ ##
##                     ##



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
  
 data_type = "B"
 monitor_type = "2"
 verbose = "1"
 parameters = c("OZONE", "PM25", "PM10", "CO", "NO2", "SO2")
 include_raw_concentrations = "1"
 api_key = epa_key

  root_url <- "https://www.airnowapi.org/aq/data/?"

  parameter_url <- paste0("format=", "application/json",
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
    df <- content(response, as = "text") %>%
      fromJSON()
  } else {
    stop("API request failed")
  }
  return (df)
}

# Transforms the EPA data from long to wide format and renames the columns to 
# be synonymous with PurpleAir's data. 
transform_epa <- function(df) {
  
  # FullAQSCode is redundant. there's another field called IntlAQSCode that
  # contains the FullAQSCode and also the country code.
  df$FullAQSCode <- NULL 
  
  # epa uses -999 to signify empty values. I've also seen -999.0 so replace
  # both to be safe.
  df[df == -999] <- NA
  df[df == -999.0] <- NA
    
  # converts the epa data from long to wide form.
  df <- df %>% pivot_wider(names_from=Parameter, values_from=c(Value, AQI, Category, RawConcentration, Unit)) 
  
  # vector used to rename the epa fields to match purple air.
  to_rename <- c(latitude = "Latitude",
                 longitude= "Longitude",
                 sensor_index = "IntlAQSCode",
                 time_stamp = "UTC",
                 name = "SiteName",
                 agency = "AgencyName",
                 pm2.5_aqi = "AQI_PM2.5",
                 no2_aqi = "AQI_NO2",
                 co_aqi = "AQI_CO",
                 ozone_aqi = "AQI_OZONE",
                 pm10.0_aqi = "AQI_PM10",
                 so2_aqi = "AQI_SO2",
                 pm2.5_category = "Category_PM2.5", 
                 no2_category = "Category_NO2",
                 co_category = "Category_CO",
                 ozone_category = "Category_OZONE",
                 pm10.0_category = "Category_PM10",
                 so2_category = "Category_SO2",
                 pm2.5_unit = "Unit_PM2.5", 
                 no2_unit = "Unit_NO2",
                 co_unit = "Unit_CO",
                 ozone_unit = "Unit_OZONE",
                 pm10.0_unit = "Unit_PM10",
                 so2_unit = "Unit_SO2",
                 pm2.5_60minute = "Value_PM2.5", 
                 no2_60minute = "Value_NO2",
                 co_60minute = "Value_CO",
                 ozone_60minute = "Value_OZONE",
                 pm10.0_60minute = "Value_PM10",
                 so2_60minute = "Value_SO2",
                 pm2.5_atm = "RawConcentration_PM2.5", 
                 no2_atm = "RawConcentration_NO2",
                 co_atm = "RawConcentration_CO",
                 ozone_atm = "RawConcentration_OZONE",
                 pm10.0_atm = "RawConcentration_PM10",
                 so2_atm = "RawConcentration_SO2")
  
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



##  ____                   _         _    _       ##
## |  _ \ _   _ _ __ _ __ | | ___   / \  (_)_ __  ##
## | |_) | | | | '__| '_ \| |/ _ \ / _ \ | | '__| ##
## |  __/| |_| | |  | |_) | |  __// ___ \| | |    ##
## |_|    \__,_|_|  | .__/|_|\___/_/   \_|_|_|    ##
##                  |_|                           ##



# grabs pa data from the purpleair api. requires an api key with points.
get_pa <- function(nwlng, nwlat, selng, selat, location, api_key) {
  # PurpleAir API URL
  root_url <- "https://api.purpleair.com/v1/sensors/"
  
  # Box domain: lat_lon = [nwlng,, nwlat, selng, selat]
  lat_lon <- c(nwlng, nwlat, selng, selat)
  ll_api_url <- paste0("&nwlng=", lat_lon[1], "&nwlat=", lat_lon[2], "&selng=", lat_lon[3], "&selat=", lat_lon[4])
  
  fields_list <- c("sensor_index", 
                   "last_seen", 
                   "name", 
                   "latitude", 
                   "longitude", 
                   "humidity_a", "humidity_b", 
                   "temperature_a", "temperature_b",
                   "pressure_a", "pressure_b",
                   "pm2.5_cf_1_a", "pm2.5_cf_1_b",
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
    names(df) <- json_data$fields
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
                           'pm2.5_60minute_cf_1_a', 'pm2.5_60minute_cf_1_b'), as.numeric)  
  
  # grabbing _a and _b channels is the same cost as grabbing the averaged so this
  # gives us more data and also a way to test confidence of data.
  df$humidity <- rowMeans(df[, c('humidity_a', 'humidity_b')])
  df$temperature <- rowMeans(df[, c('temperature_a', 'temperature_b')])
  df$pressure <- rowMeans(df[, c('pressure_a', 'pressure_b')])
  df$pm2.5_cf_1_60minute <- rowMeans(df[, c('pm2.5_cf_1_60minute_a', 'pm2.5_cf_1_60minute_b')])
  
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
                           'pm2.5_cf_1_a', 'pm2.5_cf_1_b',
                           'confidence'), as.numeric)  
  
  # grabbing _a and _b channels is the same cost as grabbing the averaged so this
  # gives us more data and also a way to test confidence of data.
  df$humidity <- rowMeans(df[, c('humidity_a', 'humidity_b')], na.rm=T)
  df$temperature <- rowMeans(df[, c('temperature_a', 'temperature_b')], na.rm=T)
  df$pressure <- rowMeans(df[, c('pressure_a', 'pressure_b')], na.rm=T)
  df$pm2.5_cf_1 <- rowMeans(df[, c('pm2.5_cf_1_a', 'pm2.5_cf_1_b')])
  
  # corrected PA pm2.5 to get more inline with EPA pm2.5
  df <- within(df, pm2.5_adjusted <- (-57.32) + 0.41 * pm2.5_cf_1 + (-0.060) * humidity + 0.068 * pressure + 0.047 * temperature)
  
  df$pm2.5_aqi[df$pm2.5_adjusted <= 500.4] <- con2aqi("pm25", df$pm2.5_adjusted[df$pm2.5_adjusted <= 500.4])
  
  df$source <- "PurpleAir"
  
  return(df)
}



##     _    ___  __  __          _      ##
##    / \  / _ \|  \/  | ___ ___| |__   ##
##   / _ \| | | | |\/| |/ _ / __| '_ \  ##
##  / ___ | |_| | |  | |  __\__ | | | | ## 
## /_/   \_\__\_|_|  |_|\___|___|_| |_| ## 
##                                      ##

transform_aqmesh <- function(df) {
  
}



load(input_df)

epa_time <- 1
while (T) {
  print("grabbing PurpleAir data")
  df_pa <- get_pa(nwlng = pa_nwlng, nwlat = pa_nwlat, selng = pa_selng, selat = pa_selat,
                  location, api_key=pa_key)
  print("transforming PurpleAir data")
  df_pa <- transform_pa(df_pa)
  
  if (epa_time %% 6 == 0) {
    print("grabbing EPA data")
    df_epa <- get_epa(nwlng = epa_nwlng, nwlat = epa_nwlat, selng = epa_selng, selat = epa_selat,
                             api_key = epa_key)
    print("transforming EPA data")
    df_epa <- transform_epa(df_epa) 
    
    print("joining PurpleAir and EPA data")
    df_new <- list(df_pa, df_epa) %>% reduce(full_join, by=c('sensor_index', 
                                                             'time_stamp', 
                                                             'latitude', 
                                                             'longitude', 
                                                             'name', 
                                                             'source',
                                                             'pm2.5_aqi'))
    epa_time <- 1
  } else {
    df_new <- df_pa
  }
  
  df_new$type <- "real-time"
  
  dbAppendTable(con, "df_all", path_to_db)
  
  # print("merging real-time with historical data")
  # df_all <- bind_rows(df_all, df_new)
  # 
  # print("removing duplicate rows")
  # df_all <- df_all %>% distinct()
  # 
  # print("saving data")
  # save(df_all, file=out_file)

  print("data saved. sleeping 10 minutes")
  
  epa_time <- epa_time + 1
  Sys.sleep(600) # Sleep 10m
}

  # df_pa <- transform_pa_historical(df_pa)
  # 
  # df_epa <- transform_epa(df_epa)
  # df_new <- df_epa
  # 
  # df_all$pm2.5_aqi[!is.na(df_all$pm2.5_60minute) & df_all$pm2.5_60minute <= 500.4 & is.na(df_all$pm2.5_aqi)] <- con2aqi("pm25", df_all$pm2.5_60minute[!is.na(df_all$pm2.5_60minute) & df_all$pm2.5_60minute <= 500.4 & is.na(df_all$pm2.5_aqi)])
  # df_all$pm10.0_aqi[!is.na(df_all$pm10.0_60minute) & df_all$pm10.0_60minute <= 604 & is.na(df_all$pm10.0_aqi)] <- con2aqi("pm10", df_all$pm10.0_60minute[!is.na(df_all$pm10.0_60minute) & df_all$pm10.0_60minute <= 604 & is.na(df_all$pm10.0_aqi)])
  # save(df_all, file="out/combined_aqis.Rda")
  # 
  # load(file="out/combined_aqis.Rda")
  # 
  # df_all$pm2.5_60minute[is.na(df_all$pm2.5_60minute)] <- rowMeans(df_all[is.na(df_all$pm2.5_60minute), c("pm2.5_60minute_a", "pm2.5_60minute_b")])
  # df_all$pm10.0_60minute[is.na(df_all$pm10.0_60minute)] <- rowMeans(df_all[is.na(df_all$pm10.0_60minute), c("pm10.0_60minute_a", "pm10.0_60minute_b")])
  # df_all$pm1.0_60minute[is.na(df_all$pm1.0_60minute)] <- rowMeans(df_all[is.na(df_all$pm1.0_60minute), c("pm1.0_60minute_a", "pm1.0_60minute_b")])
  # df_all$temperature[is.na(df_all$temperature)] <- rowMeans(df_all[is.na(df_all$temperature), c("temperature_a", "temperature_b")])
  # df_all$humidity[is.na(df_all$humidity)] <- rowMeans(df_all[is.na(df_all$humidity), c("humidity_a", "humidity_b")])
  # df_all$pressure[is.na(df_all$pressure)] <- rowMeans(df_all[is.na(df_all$pressure), c("pressure_a", "pressure_b")])
  # 
  # df_all <- df_all
  # 
  save(df_all, file="out/df_all.Rda")
  # load(file="out/df_all.Rda")
  
  # df_all$humidity <- rowMeans(df_all[, c('humidity_a', 'humidity_b')], na.rm=T)
  # df_all$temperature <- rowMeans(df_all[, c('temperature_a', 'temperature_b')], na.rm=T)
  # df_all$pressure <- rowMeans(df_all[, c('pressure_a', 'pressure_b')], na.rm=T)
  # 
  # df_all$pm2.5_aqi[is.na(df_all$pm2.5_aqi) & !is.na(df_all$pm2.5_aqi.x)] <- df_all$pm2.5_aqi.x[is.na(df_all$pm2.5_aqi) & !is.na(df_all$pm2.5_aqi.x)]
  # df_all$pm2.5_atm[is.na(df_all$pm2.5_atm) & !is.na(df_all$pm2.5_atm.x)] <- df_all$pm2.5_atm.x[is.na(df_all$pm2.5_atm) & !is.na(df_all$pm2.5_atm.x)]
  # df_all$pm2.5_aqi[is.na(df_all$pm10.0_aqi) & !is.na(df_all$pm10.0_aqi.x)] <- df_all$pm10.0_aqi.x[is.na(df_all$pm10.0_aqi) & !is.na(df_all$pm10.0_aqi.x)]
  # df_all$pm2.5_atm[is.na(df_all$pm10.0_atm) & !is.na(df_all$pm10.0_atm.x)] <- df_all$pm10.0_atm.x[is.na(df_all$pm10.0_atm) & !is.na(df_all$pm10.0_atm.x)]
  # 
  # df_all$pm2.5_aqi[is.na(df_all$pm2.5_aqi) & !is.na(df_all$pm2.5_aqi.y)] <- df_all$pm2.5_aqi.y[is.na(df_all$pm2.5_aqi) & !is.na(df_all$pm2.5_aqi.y)]
  # df_all$pm2.5_atm[is.na(df_all$pm2.5_atm) & !is.na(df_all$pm2.5_atm.y)] <- df_all$pm2.5_atm.y[is.na(df_all$pm2.5_atm) & !is.na(df_all$pm2.5_atm.y)]
  # df_all$pm2.5_aqi[is.na(df_all$pm10.0_aqi) & !is.na(df_all$pm10.0_aqi.y)] <- df_all$pm10.0_aqi.y[is.na(df_all$pm10.0_aqi) & !is.na(df_all$pm10.0_aqi.y)]
  # df_all$pm2.5_atm[is.na(df_all$pm10.0_atm) & !is.na(df_all$pm10.0_atm.y)] <- df_all$pm10.0_atm.y[is.na(df_all$pm10.0_atm) & !is.na(df_all$pm10.0_atm.y)]
  # 
  # df_all$pm2.5_aqi.x <- NULL
  # df_all$pm2.5_atm.x <- NULL
  # df_all$pm10.0_aqi.x <- NULL
  # df_all$pm10.0_atm.x <- NULL
  # 
  # df_all[df_all$source == "PurpleAir",] <- within(df_all[df_all$source == "PurpleAir",], pm2.5_aqi <- (-57.32) + 0.41 * pm2.5_cf_1_60minute + (-0.060) * humidity + 0.068 * pressure + 0.047 * temperature)
  
  
