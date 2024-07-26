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

# by default, if database is busy sqlite will just error instead of trying to wait to write to it
# this tells the database to wait 60 seconds.
RSQLite::sqliteSetBusyHandler(con, 60000)



##  _____ ____   _     ##
## | ____|  _ \ / \    ##
## |  _| | |_) / _ \   ##
## | |___|  __/ ___ \  ##
## |_____|_| /_/   \_\ ##
##                     ##


import_epa <- function(path) {
  df <- list.files(path = path,  # Identify all CSV files
                   pattern = "*.csv", full.names = TRUE) %>% 
    lapply(read_csv, na = c(-999,-999.0, "NA"), comment = ",,,,,") %>% 
    bind_rows     
  
  return(df)
}

# Function to pull data from the EPA's AirNow EPA. Requires an AirNow account
# and an API key.
get_epa <- function(nwlng, nwlat,  selng, selat,
                    data_type = "B", monitor_type = "2", verbose = "1",
                    parameters = c("OZONE", "PM25", "PM10", "CO", "NO2", "SO2"),
                    api_key, start_date, end_date) {

  # epa allows you to select an area for sensors. this creates that from 2 lats
  # and 2 longs
  bbox <- paste(nwlng, nwlat, selng, selat, sep=",")
  
 data_type = "B"
 monitor_type = "2"
 verbose = "1"
 parameters = c("OZONE", "PM25", "PM10", "CO", "NO2", "SO2")
 include_raw_concentrations = "0"
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
  df <- df %>% pivot_wider(names_from=Parameter, values_from=c(Value, AQI, Category, Unit)) 
  
  # vector used to rename the epa fields to match purple air.
  to_rename <- c(latitude = "Latitude",
                 longitude= "Longitude",
                 sensor_index = "IntlAQSCode",
                 time_stamp = "UTC",
                 name = "SiteName",
                 agency = "AgencyName",
                 pm2.5_aqi_dashboard = "AQI_PM2.5",
                 no2_aqi_dashboard = "AQI_NO2",
                 co_aqi_dashboard = "AQI_CO",
                 ozone_aqi_dashboard = "AQI_OZONE",
                 pm10.0_aqi_dashboard = "AQI_PM10",
                 so2_aqi_dashboard = "AQI_SO2",
                 pm2.5_category_dashboard = "Category_PM2.5", 
                 no2_category_dashboard = "Category_NO2",
                 co_category_dashboard = "Category_CO",
                 ozone_category_dashboard = "Category_OZONE",
                 pm10.0_category_dashboard = "Category_PM10",
                 so2_category_dashboard = "Category_SO2",
                 pm2.5_dashboard = "Value_PM2.5", 
                 no2_dashboard = "Value_NO2",
                 co_dashboard = "Value_CO",
                 ozone_dashboard = "Value_OZONE",
                 pm10.0_dashboard = "Value_PM10",
                 so2_dashboard = "Value_SO2",
                 pm2.5_unit = "Unit_PM2.5", 
                 no2_unit = "Unit_NO2",
                 co_unit = "Unit_CO",
                 ozone_unit = "Unit_OZONE",
                 pm10.0_unit = "Unit_PM10",
                 so2_unit = "Unit_SO2",
                 pm2.5_raw = "RawConcentration_PM2.5", 
                 no2_raw = "RawConcentration_NO2",
                 co_raw = "RawConcentration_CO",
                 ozone_raw = "RawConcentration_OZONE",
                 pm10.0_raw = "RawConcentration_PM10",
                 so2_raw = "RawConcentration_SO2")
  
  # originally, i had issues with the program erroring and crashing if a field
  # was absent that was trying to be renamed. using any_of and a character
  # vector fixed that.
  df <- df %>% rename(any_of(to_rename))
  
  df[, c('no2_raw', 'co_raw', 'so2_raw', 'ozone_raw', 'pm2.5_raw', 'pm10.0_raw')] <- list(NULL)
  df[, c('no2_unit', 'co_unit', 'so2_unit', 'ozone_unit', 'pm2.5_unit', 'pm10.0_unit')] <- list(NULL)
  
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

import_pa <- function(path) {
  df <- list.files(path = path,  # Identify all CSV files
                   pattern = "*.csv", full.names = TRUE) %>% 
    lapply(read_csv, na = c("", "null", "NA"), comment = ",,,,,") %>% 
    bind_rows     
  
  return(df)
}

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

transform_pa <- function(df, useAdjusted=T) {
  
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
                           'pm2.5_cf_1_a', 'pm2.5_cf_1_b'), as.numeric)  
  df <- df %>%
    mutate_at(vars(one_of('confidence')), as.numeric) 
  
  # grabbing _a and _b channels is the same cost as grabbing the averaged so this
  # gives us more data and also a way to test confidence of data.
  df$humidity_dashboard <- rowMeans(df[, c('humidity_a', 'humidity_b')], na.rm=T)
  df$temperature_dashboard <- rowMeans(df[, c('temperature_a', 'temperature_b')], na.rm=T)
  df$pressure_dashboard <- rowMeans(df[, c('pressure_a', 'pressure_b')], na.rm=T)
  
  df$pm2.5_cf_1 <- rowMeans(df[, c('pm2.5_cf_1_a', 'pm2.5_cf_1_b')])
  
  # corrected PA pm2.5 to get more inline with EPA pm2.5
  df <- within(df, pm2.5_adjusted_a <- (-58.20) + (0.41 * pm2.5_cf_1_a) + (-0.061 * humidity_dashboard) + (0.069 * pressure_dashboard) + (0.048 * temperature_dashboard))
  df <- within(df, pm2.5_adjusted_b <- (-58.20) + (0.41 * pm2.5_cf_1_b) + (-0.061 * humidity_dashboard) + (0.069 * pressure_dashboard) + (0.048 * temperature_dashboard))
  
  df$pm2.5_adjusted <- rowMeans(df[, c('pm2.5_adjusted_a', 'pm2.5_adjusted_b')])
  
    
  if (useAdjusted) {
    df$pm2.5_a_dashboard <- df$pm2.5_adjusted_a
    df$pm2.5_b_dashboard <- df$pm2.5_adjusted_b
    df$pm2.5_dashboard <- df$pm2.5_adjusted
    
    df$pm2.5_aqi_dashboard[df$pm2.5_adjusted <= 500.4 & !is.na(df$pm2.5_adjusted)] <- con2aqi("pm25", df$pm2.5_adjusted[df$pm2.5_adjusted <= 500.4 & !is.na(df$pm2.5_adjusted)])
    df$pm2.5_aqi_dashboard[df$pm2.5_adjusted_aqi < 0] <- 0
  } else {
    df$pm2.5_a_dashboard <- df$pm2.5_cf_1_a
    df$pm2.5_b_dashboard <- df$pm2.5_cf_1_b
    df$pm2.5_dashboard <- df$pm2.5_cf_1
    
    df$pm2.5_aqi_dashboard[df$pm2.5_cf_1 <= 500.4 & !is.na(df$pm2.5_cf_1)] <- con2aqi("pm25", df$pm2.5_cf_1[df$pm2.5_cf_1 <= 500.4 & !is.na(df$pm2.5_cf_1)])
  }
  
  df$source <- "PurpleAir"
  
  return(df)
}



##     _    ___  __  __          _      ##
##    / \  / _ \|  \/  | ___ ___| |__   ##
##   / _ \| | | | |\/| |/ _ / __| '_ \  ##
##  / ___ | |_| | |  | |  __\__ | | | | ## 
## /_/   \_\__\_|_|  |_|\___|___|_| |_| ## 
##                                      ##

import_aqmesh <- function(path) {
  df <- list.files(path = path,  # Identify all CSV files
                   pattern = "*.csv", full.names = TRUE) %>% 
    lapply(read_csv, na = c("", "NA"), comment = ",,,,,") %>% 
    bind_rows     
  
  return(df)
}

transform_aqmesh <- function(df) {
  
  # vector used to rename fields.
  to_rename <- c(latitude = "Latitude",
                 longitude = "Longitude",
                 time_stamp = "Timestamp",
                 name = "Session_Name",
                 temperature_dashboard = "1:Measurement_Value",
                 pm1.0_dashboard = "2:Measurement_Value",
                 pm10.0_dashboard = "3:Measurement_Value",
                 pm2.5_dashboard = "4:Measurement_Value",
                 humidity_dashboard = "5:Measurement_Value")
  
  df <- df %>% rename(any_of(to_rename))
  
  df$ObjectID <- NULL
  
  df$source <- "AQMesh"
  
  return(df)
}












# If the table in the database has columns that the data being added to it doesn't
# have then it will error. This query just gives an empty dataframe that has the
# structure of the database in order row_bind with the data so as to allow it to
# be written to the database.
db_cols <- dbFetch(dbSendQuery(con, "SELECT * FROM df_all LIMIT 0"))
db_cols$time_stamp <- as.POSIXct(db_cols$time_stamp)

epa_time <- 1
while (T) {
    tryCatch({ 
      print("grabbing PurpleAir data")
      df_pa <- get_pa(nwlng = pa_nwlng, nwlat = pa_nwlat, selng = pa_selng, selat = pa_selat,
                      location, api_key=pa_key)
      print("transforming PurpleAir data")
      df_pa <- transform_pa(df_pa, useAdjusted)
      df_pa$type <- "real-time"
      
      print("writing PA data to database")
      df_pa <- bind_rows(df_pa, db_cols)
      dbWriteTable(con, "df_all", df_pa, append=T)
    }, error = function(err) {
      message(paste("ERROR:", err, ". Trying again in 10 minutes."))
    })
  
  if (epa_time %% 6 == 0) {
    tryCatch({ 
      print("grabbing EPA data")
      df_epa <- get_epa(nwlng = epa_nwlng, nwlat = epa_nwlat, selng = epa_selng, selat = epa_selat,
                               api_key = epa_key)
      
      print("transforming EPA data")
      df_epa <- transform_epa(df_epa) 
      
      df_epa$type <- "real-time"
    
      print("writing EPA data to database")
      df_epa <- bind_rows(df_epa, db_cols)
      dbWriteTable(con, "df_all", df_epa, append=T)
      
      epa_time <- 1
    }, error = function(err) {
      message(paste("ERROR:", err, ". Trying again in 10 minutes."))
    })
  }
  
  print("data saved. sleeping 10 minutes")
  epa_time <- epa_time + 1
  Sys.sleep(600) # Sleep 10m
}