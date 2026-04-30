library(duckdb)
setwd("/Users/zhanchaoyang/Desktop/FARS/zy/Tranditional_data")
con <- dbConnect(duckdb::duckdb(), "raw_data.duckdb")

csv_path2023 <- "/Users/zhanchaoyang/Desktop/FARS/zy/Tranditional_data/person_2023.csv"
dbExecute(con, sprintf("CREATE TABLE person2023 AS SELECT * FROM read_csv_auto('%s')", csv_path2023))

csv_path2024 <- "/Users/zhanchaoyang/Desktop/FARS/zy/Tranditional_data/person_2024.csv"
dbExecute(con, sprintf("CREATE TABLE person2024 AS SELECT * FROM read_csv_auto('%s')", csv_path2024))

dbListTables(con)

dbDisconnect(con, shutdown = TRUE)
