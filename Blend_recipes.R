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


ticket_info <-
DBI::dbGetQuery(con, 'SELECT grade_internal, grade_internal_reclass, mass, 
                      calc_total_value_dollars, status_processed, grade_processed, location_current
  
                      FROM tickets T1
    
                        LEFT JOIN processing_blends T2 ON
                        T2.\"__kp_processing_blend_id\" = T1.\"_ext_processing_blend_id\"

                        WHERE status_tickets = \'SOLD\'
                         
                         ')


# shows what has been swapped in and what has been swapped out


## This is all swap outs

ticket_info %>% as_tibble %>% filter(grepl("SHIPMENTS", grade_processed)) %>% select(-grade_internal, -location_current) %>%
 group_by(grade_internal_reclass, grade_processed) %>% summarize(mass = sum(mass), value = sum(calc_total_value_dollars)) %>%
 arrange(desc(mass)) %>% mutate(status = "OUT") %>% bind_rows(

## this is all swap ins

ticket_info %>% as_tibble %>% filter(location_current == "MTC - SWAP STOCKS") %>%
  group_by(grade_internal_reclass, location_current) %>% summarize(mass = sum(mass), value = sum(calc_total_value_dollars)) %>%
  rename(grade_processed = location_current) %>%
  arrange(desc(mass)) %>% mutate(status = "IN")) %>% rmsfuns::ViewXL()


# Price of remaining unprocessed green

## this also gets to the starting point of green purchased and net of swaps


ticket_info %>% as_tibble() %>% mutate(grade_processed = ifelse(is.na(grade_processed), "UNPROCESSED", grade_processed)) %>%
  filter(grade_internal != "TBDAU") %>%
  
  bind_rows(
    ticket_info %>% as_tibble() %>% mutate(grade_processed = ifelse(is.na(grade_processed), "UNPROCESSED", grade_processed)) %>%
      filter(grade_internal == "TBDAU") %>% filter(location_current %in% c("BAY 12", "TPZ", "MTC - SWAP STOCKS"))
  ) %>%
    
  group_by(grade_processed) %>% summarize(mass = sum(mass), value = sum(calc_total_value_dollars)) %>%
  mutate(price = value/mass) %>% filter(!grepl("SHIPMENTS", grade_processed)) %>%
  rmsfuns::ViewXL()


# Breakdown of unprocessed

ticket_info %>% as_tibble() %>% mutate(grade_processed = ifelse(is.na(grade_processed), "UNPROCESSED", grade_processed)) %>%
  filter(grade_internal != "TBDAU") %>%
  
  bind_rows(
    ticket_info %>% as_tibble() %>% mutate(grade_processed = ifelse(is.na(grade_processed), "UNPROCESSED", grade_processed)) %>%
      filter(grade_internal == "TBDAU") %>% filter(location_current %in% c("BAY 12", "MTC - SWAP STOCKS"))) %>% 
  
  filter(grade_processed == "UNPROCESSED") %>%
  group_by(grade_internal_reclass) %>% summarise(mass = sum(mass), value = sum(calc_total_value_dollars)) %>%
  arrange(desc(mass)) %>% mutate(price = value/mass) %>%
  rmsfuns::ViewXL()



recipes <-
  DBI::dbGetQuery(con, "SELECT * FROM processing_recipe T1
                  
                  LEFT JOIN processing_blends T2 ON
                        T2.\"__kp_processing_blend_id\" = T1.\"_ext_processing_blend_id\"") %>% clean_names()



# Still in testing - automated remaining green calculations

  
recipes %>% select(grade_internal, calc_precent_grade_internal_share, grade_processed) %>% 
  left_join(
    
    ticket_info %>% as_tibble() %>% mutate(grade_processed = ifelse(is.na(grade_processed), "UNPROCESSED", grade_processed)) %>%
      filter(grade_internal != "TBDAU") %>%
      
      bind_rows(
        ticket_info %>% as_tibble() %>% mutate(grade_processed = ifelse(is.na(grade_processed), "UNPROCESSED", grade_processed)) %>%
          filter(grade_internal == "TBDAU") %>% filter(location_current %in% c("BAY 12", "MTC - SWAP STOCKS"))
      ) %>% filter(grade_processed == "UNPROCESSED") %>%
      group_by(grade_internal_reclass) %>% summarise(mass = sum(mass), value = sum(calc_total_value_dollars)) %>%
      arrange(desc(mass)) %>% mutate(price = value/mass), by = c("grade_internal" = "grade_internal_reclass")) %>%
  mutate(mass_2 = mass*(calc_precent_grade_internal_share/100), value_2 = value*(calc_precent_grade_internal_share/100)) %>% na.omit() %>%
  group_by(grade_processed) %>% summarise(mass = sum(mass_2), value = sum(value_2)) %>% mutate(price = value/mass) %>%
  rmsfuns::ViewXL()



# Breakdown of auction


ticket_info %>% as_tibble() %>% mutate(grade_processed = ifelse(is.na(grade_processed), "UNPROCESSED", grade_processed)) %>%
  filter(grade_internal == "TBDAU") %>%
  group_by(location_current) %>% summarise(mass = sum(mass))


ticket_info %>% as_tibble() %>% mutate(grade_processed = ifelse(is.na(grade_processed), "UNPROCESSED", grade_processed)) %>%
  filter(grade_internal == "TBDAU") %>%
  filter(location_current == "TPZ") %>%
  rmsfuns::ViewXL()







# extracting actual blend recepies for optimizer


ticket_info %>% 
  
  mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                         
                                         ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                grade_internal_reclass),
                                         
                                         grade_internal_reclass)) %>%
  
  group_by(grade_processed, grade_internal_reclass) %>% summarise(mass = sum(mass, na.rm = TRUE)) %>% 
  
  rmsfuns::ViewXL()




# Auciton percentage for presentation


ticket_info %>% 
  
  mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                         
                                         ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                grade_internal_reclass),
                                         
                                         grade_internal_reclass)) %>%
  
  group_by(grade_processed, grade_internal) %>% summarise(mass = sum(mass, na.rm = TRUE)) %>%
  
  mutate(mass2 = sum(mass)) %>% mutate(per = (mass/mass2)) %>% filter(grade_internal == "TBDAU") %>%
  
  rmsfuns::ViewXL()



# Stem breakdown for presentation


ticket_info %>% 
  
  mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                         
                                         ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                grade_internal_reclass),
                                         
                                         grade_internal_reclass)) %>%
  
  group_by(grade_processed, grade_internal_reclass) %>% summarise(mass = sum(mass, na.rm = TRUE)) %>%
  
  filter(grade_internal_reclass %in% c("FTU", "FT")) %>%
  
  rmsfuns::ViewXL()
  



# Reclass Exercise


ticket_info %>% group_by(grade_internal, grade_internal_reclass) %>% summarise(mass = sum(mass, na.rm = TRUE)) %>%
  mutate(check = ifelse(grade_internal == grade_internal_reclass, TRUE, FALSE)) %>%
  filter(check == FALSE) %>% filter(grade_internal != "TBDAU") %>%
  rmsfuns::ViewXL()



# Ticket recon


ticket_recon <-
  DBI::dbGetQuery(con, 'SELECT \"_ext_number_ticket\", date_sale, grade_internal, grade_internal_reclass, mass, 
                      calc_total_value_dollars, status_processed, grade_processed, location_current
  
                      FROM tickets T1
    
                        LEFT JOIN processing_blends T2 ON
                        T2.\"__kp_processing_blend_id\" = T1.\"_ext_processing_blend_id\"

                        WHERE status_tickets = \'SOLD\'
                         
                         ')


# Breakdown of unprocessed

ticket_recon %>% as_tibble() %>% mutate(grade_processed = ifelse(is.na(grade_processed), "UNPROCESSED", grade_processed)) %>%
  filter(grade_internal != "TBDAU") %>%
  
  bind_rows(
    ticket_recon %>% as_tibble() %>% mutate(grade_processed = ifelse(is.na(grade_processed), "UNPROCESSED", grade_processed)) %>%
      filter(grade_internal == "TBDAU") %>% filter(location_current %in% c("BAY 12", "MTC - SWAP STOCKS"))
  ) %>% filter(grade_processed == "UNPROCESSED") %>%
  rmsfuns::ViewXL()



ticket_recon %>% filter(grade_processed == "FF3RMS") %>%
  group_by(grade_internal_reclass) %>% summarise(mass = sum(mass), value = sum(calc_total_value_dollars)) %>%
  rmsfuns::ViewXL()
