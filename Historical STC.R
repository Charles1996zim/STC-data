library(tidyverse)
library(odbc)
library(dplyr)
library(DBI)

options("scipen"=10) 



con19 <- DBI::dbConnect(odbc::odbc(),
                      driver = "FileMaker ODBC",
                      server = "db02.nt.co.zw", # 127.0.0.1
                      database = "STC 2019.fmp12",
                      uid = "CRandell",
                      pwd = "Ngenile!99")

tickets_2019 <-
DBI::dbGetQuery(con19, "
                SELECT grade_internal, sum(mass) AS 'Mass', sum(calc_total_price_dollars) AS 'value'
                
                FROM tickets
                
                WHERE status = 'SOLD'
                
                GROUP BY grade_internal")


con20 <- DBI::dbConnect(odbc::odbc(),
                        driver = "FileMaker ODBC",
                        server = "db02.nt.co.zw", # 127.0.0.1
                        database = "STC 2020.fmp12",
                        uid = "CRandell",
                        pwd = "Ngenile!99")


tickets_2020 <-
  DBI::dbGetQuery(con20, "
                SELECT grade_internal, sum(mass) AS 'Mass', sum(calc_total_value_dollars) AS 'value'
                
                FROM tickets
                
                WHERE status_tickets = 'SOLD'
                
                GROUP BY grade_internal")


tickets_2019 %>% full_join(tickets_2020, by = "grade_internal", suffix = c("2019", "2020")) %>%
  filter(!grepl("/", grade_internal)) %>%
  rmsfuns::ViewXL()
                







  DBI::dbGetQuery(con19, "
                SELECT *
                
                FROM tickets
                
                WHERE status = 'SOLD'
                  
                FETCH FIRST 100 ROWS ONLY")
