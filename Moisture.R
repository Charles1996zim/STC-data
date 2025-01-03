library(tidyverse)
library(odbc)
library(dplyr)
library(DBI)
library(janitor)

options("scipen"=10) 



con <- DBI::dbConnect(odbc::odbc(),
                      driver = "FileMaker ODBC",
                      server = "db02.nt.co.zw", # 127.0.0.1
                      database = "STC 2023.fmp12",
                      uid = "CRandell",
                      pwd = "Ngenile!99")



## This query gives all the tickets with theri current locations and status in terms of processing

ticket_info_m <-
  DBI::dbGetQuery(con, 'SELECT grade_internal, grade_internal_reclass, mass, 
                      calc_total_value_dollars, status_processed, grade_processed, location_current, moisture_mean
  
                      FROM tickets T1
    
                        LEFT JOIN processing_blends T2 ON
                        T2.\"__kp_processing_blend_id\" = T1.\"_ext_processing_blend_id\"

                        WHERE status_tickets = \'SOLD\'
                         
                         ')


# shows what has been swapped in and what has been swapped out


# Price of remaining unprocessed green

## this also gets to the starting point of green purchased and net of swaps


ticket_info_m %>% as_tibble() %>% mutate(grade_processed = ifelse(is.na(grade_processed), "UNPROCESSED", grade_processed)) %>%
  filter(grade_internal != "TBDAU") %>%
  
  bind_rows(
    ticket_info_m %>% as_tibble() %>% mutate(grade_processed = ifelse(is.na(grade_processed), "UNPROCESSED", grade_processed)) %>%
      filter(grade_internal == "TBDAU") %>% filter(location_current %in% c("BAY 12", "TPZ", "MTC - SWAP STOCKS"))
  ) %>%
  
  mutate(moisture = mass*(moisture_mean/100)) %>%
  group_by(grade_processed) %>% summarize(moisture = sum(moisture, na.rm = TRUE)) %>%
  filter(!grepl("SHIPMENTS", grade_processed)) %>%
  rmsfuns::ViewXL()
