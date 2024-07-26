#!/usr/bin/env Rscript

# Description: this script just converts from an Rda file to an sqlite database.
# It's pretty useful because Rda is compressed enough to be committed to github,
# while the database isn't.

# WARNING: it's set to overwrite whatever table it's set to write to.

my_packages <- c("DBI",
                 "RSQLite")

not_installed <- my_packages[!(my_packages %in% installed.packages()[ , "Package"])]
if(length(not_installed)) install.packages(not_installed)    

library(DBI)

db_path <- "./out/db.sqlite"
df_path <- "./out/df_all.Rda"

load(df_path)
dbDisconnect(con)
con <- dbConnect(RSQLite::SQLite(), db_path)

dbWriteTable(con, "df_all", df_all, overwrite=T)
