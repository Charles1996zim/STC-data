library(tidyverse)
library(odbc)
library(dbplyr)
library(DBI)


con <- DBI::dbConnect(odbc::odbc(),
                      driver = "FileMaker ODBC",
                      server = "192.168.8.114", # 127.0.0.1
                      database = "STC.fmp12",
                      uid = "CRandell",
                      pwd = "Ngenile!99")

DBI::dbGetQuery(con, 'SELECT grade_internal FROM TICKETS FETCH FIRST 100 ROWS ONLY')

DBI::dbGetQuery(con, 'SELECT COUNT(*) FROM TICKETS')



grades_reclass <-
DBI::dbGetQuery(con, 
                'SELECT grade_internal, grade_internal_reclass 
                FROM TICKETS') %>% as_tibble()


grades_reclass %>% filter(grade_internal != grade_internal_reclass) %>% group_by(grade_internal, grade_internal_reclass) %>%
  summarise(n = n()) %>% arrange(desc(n))


grades_purch <-
  DBI::dbGetQuery(con, 
                  'SELECT grade_internal, grade_internal_reclass, number_price
                FROM TICKETS') %>% as_tibble()

grades_purch %>% filter(grade_internal != grade_internal_reclass) %>% group_by(grade_internal, grade_internal_reclass) %>%
  summarise(n = n()) %>% arrange(desc(n)) %>% 
  left_join(grades_purch %>% group_by(grade_internal) %>% summarise(internal = mean(number_price)), by = "grade_internal") %>%
  left_join(grades_purch %>% group_by(grade_internal) %>% summarise(reclass = mean(number_price)), by = c("grade_internal_reclass" = "grade_internal")) %>%
  mutate(price_diff = reclass - internal) %>% mutate(value_change = n*price_diff) %>% rmsfuns::ViewXL()



grades_purch %>% group_by(grade_internal_reclass) %>% summarise(m = mean(number_price, na.rm = TRUE)) %>% rmsfuns::ViewXL()



