library(tidyverse)
library(odbc)
library(dbplyr)
library(DBI)

options("scipen"=10) 



con <- DBI::dbConnect(odbc::odbc(),
                      driver = "FileMaker ODBC",
                      server = "db02.nt.co.zw", # 127.0.0.1
                      database = "STC.fmp12",
                      uid = "CRandell",
                      pwd = "Ngenile!99")



# CREATE FILE WITH ALL TICKETS

tickets <-
DBI::dbGetQuery(con, paste0("SELECT * FROM TICKETS"))

# TURN TABLE INTO TIBBLE

tickets1 <- tickets %>% as_tibble()

# FILTERS TO OBTAIN ALL COMMERICAL AND SS SALES

tickets1 %>%
  group_by(grade_internal, grade_internal_reclass, location_current) %>% 
  summarise(total = sum(mass), value = sum(calc_total_value_dollars)) %>% 
  filter(grade_internal != "TBDAU") %>%
  filter(location_current %in% c("Bay 8-9", "TPZ", "Transit - Bay 8-9", "Receiving C", "REC C (TPZ)", " TPZ", "TPZ ")) %>%
  rmsfuns::ViewXL()


tickets1 %>%
  filter(grade_internal != "TBDAU") %>% 
  filter(location_current %in% c("Bay 8-9", "TPZ", "Transit - Bay 8-9", "Receiving C", "REC C (TPZ)", " TPZ", "TPZ ")) %>%
  rmsfuns::ViewXL()


# AUCTION & HANDSTRIP

tickets1 %>% group_by(grade_internal, grade_internal_reclass, location_current, status_processed) %>% 
  summarise(total = sum(mass), value = sum(calc_total_value_dollars)) %>% 
  filter(grade_internal == "TBDAU" | grade_internal_reclass == "TBDAU") %>% 
  filter(!grepl("/", grade_internal_reclass)) %>%
  filter(!grepl("S", grade_internal_reclass)) %>%
  filter(!grepl("H", grade_internal_reclass)) %>%
  filter(!grepl("MTC", location_current)) %>%
  rmsfuns::ViewXL()

tickets1 %>% group_by(grade_internal, grade_internal_reclass, location_current, status_processed) %>% 
  summarise(total = sum(mass), value = sum(calc_total_value_dollars)) %>% 
  filter(grade_internal == "TBDAU" | grade_internal_reclass == "TBDAU") %>% 
  filter(!grepl("/", grade_internal_reclass)) %>%
  filter(!grepl("MTC", location_current)) %>% ungroup() %>%
  mutate(location_current = trimws(location_current)) %>%
  group_by(location_current) %>% summarize(total = sum(total), value = sum(value)) %>%
  rmsfuns::ViewXL()


auction_man <- readxl::read_excel("C:/Users/charlesr/Documents/Green Reconciliation/Auction data final.xlsx") %>% janitor::clean_names()


auction_man %>% left_join(tickets1 %>% select("_ext_number_ticket", 
                                              grade_internal, grade_internal_reclass, location_current, status_processed, mass, calc_total_value_dollars), 
                          by = c("nt_barcode" = "_ext_number_ticket")) %>% rmsfuns::ViewXL()


tickets1 %>%
  filter(grade_internal == "TBDAU" | grade_internal_reclass == "TBDAU") %>% 
  filter(!grepl("/", grade_internal_reclass)) %>%
  filter(!grepl("MTC", location_current)) %>% select("_ext_number_ticket", 
                                                      grade_internal, grade_internal_reclass, location_current, status_processed, mass, calc_total_value_dollars) %>%
  rmsfuns::ViewXL()


tickets1

tickets1 %>% group_by(grade_internal, grade_internal_reclass, location_current) %>% 
  summarise(total = sum(mass), value = sum(calc_total_value_dollars)) %>% 
  filter(grade_internal != "TBDAU") %>% 
  filter(location_current %in% c("Bay 8-9", "TPZ", "Transit - Bay 8-9", "Receiving C", "REC C (TPZ)", " TPZ", "TPZ ")) %>% 
  ungroup() %>%
  mutate(location_current = trimws(location_current)) %>%
  group_by(location_current) %>% summarize(total = sum(total), value = sum(value)) %>%
  rmsfuns::ViewXL()






# PROCESSING RUNS


processing <-
  DBI::dbGetQuery(con, paste0("SELECT * FROM processing_blends")) %>% as_tibble()


processing_runs <-
  DBI::dbGetQuery(con, paste0("SELECT * FROM processing_runs")) %>% as_tibble()





# LINKING TICKET NUMBER TO BLEND PROCESSSING

# ONLY NON AUCTION

tickets1 %>% left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  group_by(grade_internal, grade_internal_reclass, location_current, grade_processed) %>% 
  filter(grade_internal != "TBDAU") %>% 
  filter(location_current %in% c("Bay 8-9", "TPZ", "Transit - Bay 8-9", "Receiving C", "REC C (TPZ)", " TPZ", "TPZ ")) %>%
  group_by(grade_internal, grade_internal_reclass, grade_processed) %>% 
  summarise(total = sum(mass), value = sum(calc_total_value_dollars)) %>% 
  rmsfuns::ViewXL()


# ADJUSTS FOR MTC SWAP 

# ADDS AUCTION DATA BY ELIMINATING TICKETS WITH NO BLEND ID IN AUCITON VOLUME


  tickets1 %>% left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
    filter(!grepl("MTC", grade_processed)) %>%
    group_by(grade_internal, grade_internal_reclass, location_current, grade_processed) %>% 
    filter(grade_internal != "TBDAU") %>% 
    filter(location_current %in% c("Bay 8-9", "TPZ", "Transit - Bay 8-9", "Receiving C", "REC C (TPZ)", " TPZ", "TPZ ")) %>%
    group_by(grade_internal, grade_internal_reclass, grade_processed) %>% 
    summarise(total = sum(mass), value = sum(calc_total_value_dollars)) %>%
    bind_rows(tickets1 %>% left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
                filter(grade_internal == "TBDAU" | grade_internal_reclass == "TBDAU") %>% 
                filter(!grepl("/", grade_internal_reclass)) %>%
                filter(!grepl("MTC", grade_processed)) %>%
                filter(!grepl("HANDSTRIP", grade_processed)) %>%
                filter(!grepl("SCRAP", grade_processed)) %>%
                filter(!grepl("HSPICK", grade_processed)) %>%
                filter(grade_processed != "NA") %>%
                group_by(grade_internal, grade_internal_reclass, location_current, grade_processed) %>%
                summarise(total = sum(mass), value = sum(calc_total_value_dollars)) %>%
                ungroup() %>% select(!location_current)) %>% 
    rmsfuns::ViewXL()

  
  
  # THIS SHOWS WHERE ALL CONTRACT FLOOR TICKETS WERE ASSIGNED DURING PROCESSING 
  
  tickets1 %>%
    left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
    filter(grade_internal != "TBDAU") %>%
    filter(location_current %in% c("Bay 8-9", "TPZ", "Transit - Bay 8-9", "Receiving C", "REC C (TPZ)", " TPZ", "TPZ ")) %>%
    group_by(grade_processed) %>% summarise(total = sum(mass), value = sum(calc_total_value_dollars)) %>% rmsfuns::ViewXL()


  
# FINAL PRICE COMPARISON - WITH COMMERCIAL AND AUCTION DATA 
  
  
  
  
  tickets %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
    filter(grade_internal != "TBDAU") %>% rename(ext_processing_blend_id = "_ext_processing_blend_id") %>%
    filter(location_current %in% c("Bay 8-9", "TPZ", "Transit - Bay 8-9", "Receiving C", "REC C (TPZ)", " TPZ", "TPZ ")) %>%
    group_by(ext_processing_run_id, ext_processing_blend_id) %>% summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
    left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("ext_processing_blend_id"="__kp_processing_blend_id")) %>%
    filter(!grepl("MTC", grade_processed)) %>%
    select(mass, value, "ext_processing_run_id", grade_processed) %>%
    left_join(tickets %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
                filter(grade_internal == "TBDAU" | grade_internal_reclass == "TBDAU") %>% 
                left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
                filter(!grepl("/", grade_internal_reclass)) %>%
                filter(!grepl("MTC", grade_processed)) %>%
                filter(!grepl("HANDSTRIP", grade_processed)) %>%
                filter(!grepl("SCRAP", grade_processed)) %>%
                filter(!grepl("HSPICK", grade_processed)) %>%
                filter(grade_processed != "NA") %>%
                group_by(ext_processing_run_id) %>% summarise(mass2 = sum(mass, na.rm = TRUE), value2 = sum(calc_total_value_dollars)) %>%
                select(mass2, value2, "ext_processing_run_id"), by = c("ext_processing_run_id")) %>%
    mutate(mass2 = ifelse(is.na(mass2), 0, mass2)) %>%
    mutate(value2 = ifelse(is.na(value2), 0, value2)) %>%
    mutate(mass = mass+mass2, value = value+value2) %>% select(-mass2, -value2) %>%
    left_join(processing_runs %>% select("__kp_processing_run_id", calc_mass, calc_total_value_dollars, calc_average_price) %>%
              rename("kp_processing_run_id" = "__kp_processing_run_id"), 
              by = c("ext_processing_run_id" = "kp_processing_run_id")) %>%
    select(3, 4, everything()) %>% rmsfuns::ViewXL()
    
    

