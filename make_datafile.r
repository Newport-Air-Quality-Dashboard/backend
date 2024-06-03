library(tidyverse)


# EPA Data
pm2.5 <- read.csv("csv/epa_pm2.5.csv")
pm10 <- read.csv("csv/epa_pm10.csv")
ozone <- read.csv("csv/epa_ozone.csv")
co <- read.csv("csv/epa_co.csv")
no2 <- read.csv("csv/epa_no2.csv")
so2 <- read.csv("csv/epa_so2.csv")

epa_list <- list(pm2.5, pm10, ozone, co, no2, so2)

epa_data <- epa_list %>% reduce(full_join, by=c('time_stamp', 'sensor_index'))

rm(pm2.5, pm10, ozone, co, no2, so2)

epa_data$time_stamp <- as.POSIXct(epa_data$time_stamp, 
                                 format = "%Y-%m-%dT%H:%M",
                                 tz="UTC") 
epa_data$sensor_index <- as.character(epa_data$sensor_index)

epa_data$source <- "EPA"

str(epa_data)

save(epa_data, file="./out/epa_complete.Rda")


# PurpleAir Data
path_to_PurpleAir_data='./csv/purpleair.csv'
path_to_PurpleAir_sensor_data='./csv/purpleair_sensors.csv'

purpleair_data <- read.csv(path_to_PurpleAir_data)
purpleair_sensor_data <- read.csv(path_to_PurpleAir_sensor_data)

purpleair_data <- list(purpleair_data, purpleair_sensor_data) %>% reduce(full_join, by='sensor_index')

rm(purpleair_sensor_data)

purpleair_data$time_stamp <- as.POSIXct(purpleair_data$time_stamp, 
                                       format = "%s",
                                       tz = "UTC") 
purpleair_data$sensor_index <- as.character(purpleair_data$sensor_index)

purpleair_data$source <- "PurpleAir"

str(purpleair_data)

save(purpleair_data, file="./out/purpleair_complete.Rda")



# Combining the data sets
combined_data <- list(purpleair_data, epa_data) %>% reduce(full_join, by=c('sensor_index', 'time_stamp', 'latitude',
                                                                           'longitude', 'pm2.5_60minute', 'name', 
                                                                           'pm10.0_60minute', 'source'))

str(combined_data)

save(purpleair_data, file="./out/combined_complete.Rda")

