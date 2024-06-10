#!/usr/bin/env Rscript
# install.packages(c("httr", "jsonlite", "tidyverse"))

library(con2aqi)
library(httr)
library(jsonlite)
library(tidyverse)

source("config.R")

# Define the API key
get_EPA_data <- function(nwlng, nwlat,  selng, selat,
                         data_type = "B", monitor_type = "2", verbose = "1",
                         parameters = c("OZONE", "PM25", "PM10", "CO", "NO2", "SO2"),
                         include_raw_concentrations = "1", api_key,
                         start_date, end_date) {

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

  if(!missing(start_date) && !missing(end_date)) {
    parameter_url <- paste0(parameter_url, 
                            "&startDate=", start_date,
                            "&endDate=", end_date)
  }
  
  api_url <- paste0(root_url, parameter_url)
  
  print(api_url)

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

transform_epa <- function(df) {
  
    df$aqs_id <- NULL 
    
    df <- df %>% pivot_wider(names_from=parameter, values_from=c(concentration, aqi, category, raw_concentration, unit)) 
  
    df <- df %>% rename(pm2.5_aqi = aqi_PM2.5,
                        no2_aqi = aqi_NO2,
                        co_aqi = aqi_CO,
                        ozone_aqi = aqi_OZONE,
                        pm10.0_aqi = aqi_PM10,
                        so2_aqi = aqi_SO2,
                        pm2.5_category = category_PM2.5, 
                        no2_category = category_NO2,
                        co_category = category_CO,
                        ozone_category = category_OZONE,
                        pm10.0_category = category_PM10,
                        so2_category = category_SO2,
                        pm2.5_unit = unit_PM2.5, 
                        no2_unit = unit_NO2,
                        co_unit = unit_CO,
                        ozone_unit = unit_OZONE,
                        pm10.0_unit = unit_PM10,
                        so2_unit = unit_SO2,
                        pm2.5_60minute = concentration_PM2.5, 
                        no2_60minute = concentration_NO2,
                        co_60minute = concentration_CO,
                        ozone_60minute = concentration_OZONE,
                        pm10.0_60minute = concentration_PM10,
                        so2_60minute = concentration_SO2,
                        pm2.5_atm = raw_concentration_PM2.5, 
                        no2_atm = raw_concentration_NO2,
                        co_atm = raw_concentration_CO,
                        ozone_atm = raw_concentration_OZONE,
                        pm10.0_atm = raw_concentration_PM10,
                        so2_atm = raw_concentration_SO2)
    
    df[, c('no2_60minute', 'co_60minute', 'so2_60minute')] <- list(NULL)
    
    df$time_stamp <- as.POSIXct(df$time_stamp,
                                format = "%Y-%m-%dT%H:%M", tz="UTC") 
    df$sensor_index <- as.character(df$sensor_index)
    df$source <- "EPA"
    
    return(df)
}


get_PA_data <- function(nwlng, nwlat, selng, selat, location, api_key) {
  # PurpleAir API URL
  root_url <- "https://api.purpleair.com/v1/sensors/"
  
  # Box domain: lat_lon = [nwlng,, nwlat, selng, selat]
  lat_lon <- c(nwlng, nwlat, selng, selat)
  ll_api_url <- paste0("&nwlng=", lat_lon[1], "&nwlat=", lat_lon[2], "&selng=", lat_lon[3], "&selat=", lat_lon[4])
  
  # Fields to get
  fields_list <- c("sensor_index", "last_seen", "name", "latitude", "longitude", "humidity", "temperature", "pressure", "pm1.0", "pm2.5", "pm2.5_10minute", "pm10.0_atm", "confidence")
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

transform_purpleair <- function(df) {
  df <- df %>% rename(time_stamp = last_seen)
  df$time_stamp <- as.POSIXct(df$time_stamp, 
                              format = "%s",
                              tz = "UTC") 
  df$sensor_index <- as.character(df$sensor_index)
  df$latitude <- as.numeric(df$latitude)
  df$longitude <- as.numeric(df$longitude)
  df$source <- "PurpleAir"
  
  # df$pm2.5_aqi <- con2aqi("pm25", df$pm2.5_10minute)
  # df$pm10.0_aqi <- con2aqi("pm10", df$pm10.0_atm)
  
  return(df)
}

nwlng <- -85.825195 
nwlat <- 38.324420 
selng <- -83.023682
selat <- 39.825413

input_df="./out/combined_complete.Rda"
output_df="./out/combined_realtime.Rda"
location <- "both"

load(input_df)

epa_time <- 0
while (T) {
  print("grabbing PurpleAir data")
  PA_data <- get_PA_data(-84.534, 39.106, -84.455, 39.050, location, api_key=pa_key)
  print("transforming PurpleAir data")
  PA_data <- transform_purpleair(PA_data)
  
  if (epa_time %% 6 == 0) {
    print("grabbing EPA data")
    EPA_data <- get_EPA_data(nwlng = nwlng, nwlat = nwlat, selng = selng, selat = selat,
                             api_key = epa_key)
    print("transforming EPA data")
    EPA_data <- transform_epa(EPA_data) 
    
    print("joining PurpleAir and EPA data")
    new_data <- list(PA_data, EPA_data) %>% reduce(full_join, by=c('sensor_index', 'time_stamp', 'latitude',
                                                                   'longitude', 'name', 'source'))
    epa_time <- 0
  } else {
    new_data <- PA_data
  }
  
  new_data$type <- "real-time"
  
  print("merging real-time with historical data")
  combined_data <- bind_rows(combined_data, new_data)
  print("saving data")
  save(file=output_df)
  
  epa_time <- epa_time + 1
  Sys.sleep(600000) # Sleep 10m
}


# purpleair_data$pm2.5_aqi <- con2aqi("pm25", purpleair_data$pm2.5_60minute)
# purpleair_data$pm10.0_aqi <- con2aqi("pm10", purpleair_data$pm10.0_60minute)
