library(duckdb)
library(tidyverse)
setwd("/Users/zhanchaoyang/Desktop/FARS/zy/Tranditional_data")
con <- dbConnect(duckdb::duckdb(), dbdir = "raw_data.duckdb")

dbListTables(con)

FARS2023 <- dbGetQuery(con, "SELECT * FROM person2023")
FARS2024 <- dbGetQuery(con, "SELECT * FROM person2024")

#close connection
dbDisconnect(con, shutdown = TRUE)

## Function to clean FARS data 2023-2024 (same schema as 2015-2022 with NAME columns)

process_FARS_data <- function(data) {
  # Select relevant columns
  data_filtered <- data %>%
    select(
      ST_CASE,
      STATE,
      STATENAME,
      COUNTY,
      AGE,
      SEX,
      INJ_SEV,
      INJ_SEVNAME
    )

  # Add agegroup column
  data_filtered <- data_filtered %>%
    mutate(
      agegroup = case_when(
        AGE < 5 ~ 1,
        AGE >= 5 & AGE < 10 ~ 2,
        AGE >= 10 & AGE < 15 ~ 3,
        AGE >= 15 & AGE < 19 ~ 4,
        AGE >= 19 & AGE < 200 ~ 5,
        AGE >= 200 ~ 6
      )
    )

  # Filter rows where INJ_SEV is fatality
  data_filtered <- data_filtered %>%
    filter(INJ_SEV == 4)

  #create unique fips code
  data_filtered <- data_filtered %>%
    mutate(county_code = sprintf("%02d%03d", data_filtered$STATE, data_filtered$COUNTY))
  return(data_filtered)
}

# use the function
FARS2023_filtered <- process_FARS_data(FARS2023)
FARS2024_filtered <- process_FARS_data(FARS2024)

## No age data
FARS2023_noage <- FARS2023 %>%
  filter(AGE == 999 | AGE == 998) %>%
  select(STATE, AGE, COUNTY)

FARS2024_noage <- FARS2024 %>%
  filter(AGE == 999 | AGE == 998) %>%
  select(STATE, AGE, COUNTY)
