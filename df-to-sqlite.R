#!/usr/bin/env Rscript

library(DBI)

db_path <- "./out/df.sqlite"
df_path <- "./out/df_all.Rda"

load(df_path)

con <- dbConnect(RSQLite::SQLite(), "./out/db.sqlite")
dbWriteTable(con, "df_all", df_all, overwrite=T)
