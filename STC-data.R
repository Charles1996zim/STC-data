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

start.time <- Sys.time()

tickets <-
  DBI::dbGetQuery(con, paste0("SELECT * FROM TICKETS"))

end.time <- Sys.time()
time.taken <- end.time - start.time

processing <-
  DBI::dbGetQuery(con, paste0("SELECT * FROM processing_blends")) %>% as_tibble()


processing_runs <-
  DBI::dbGetQuery(con, paste0("SELECT * FROM processing_runs")) %>% as_tibble()

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

tickets2 %>% left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  filter(grade_internal == "TBDAU" | grade_internal_reclass == "TBDAU") %>% 
  filter(!grepl("/", grade_internal_reclass)) %>%
  filter(!grepl("MTC", location_current)) %>%
  filter(!grepl("MTC", grade_processed)) %>%
  filter(!grepl("HANDSTRIP", grade_processed)) %>%
  filter(!grepl("SCRAP", grade_processed)) %>%
  filter(!grepl("HSPICK", grade_processed)) %>%
  filter(grade_processed != "NA") %>%
  group_by(grade_internal, grade_internal_reclass, location_current, grade_processed) %>%
  summarise(total = sum(mass), value = sum(calc_total_value_dollars)) %>%
  ungroup() %>% select(!location_current) %>% rmsfuns::ViewXL()




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


tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
  filter(grade_internal != "TBDAU") %>% rename(ext_processing_blend_id = "_ext_processing_blend_id") %>%
  filter(!grepl("/P$", grade_internal_reclass)) %>%
  filter(location_current %in% c("Bay 8-9", "TPZ", "Transit - Bay 8-9", "Receiving C", "REC C (TPZ)", " TPZ", "TPZ ")) %>%
  group_by(ext_processing_run_id, ext_processing_blend_id) %>% summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
  left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  filter(!grepl("MTC", grade_processed)) %>%
  filter(!grepl("HSPICK", grade_processed)) %>%
  filter(!grepl("SCRAP", grade_processed)) %>%
  filter(!grepl("HANDSTRIP", grade_processed)) %>%
  select(mass, value, "ext_processing_run_id", grade_processed) %>%
  bind_rows(tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
              filter(grade_internal == "TBDAU" | grade_internal_reclass == "TBDAU") %>% 
              left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
              filter(!grepl("/P$", grade_internal_reclass)) %>%
              filter(!grepl("HANDSTRIP", grade_processed)) %>%
              filter(!grepl("HSPICK", grade_processed)) %>%
              filter(!grepl("MTC", grade_processed)) %>%
              filter(!grepl("SCRAP", grade_processed)) %>%
              filter(!is.na(grade_processed)) %>%
              group_by(ext_processing_run_id, grade_processed) %>% summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
              select(mass, value, "ext_processing_run_id", grade_processed)) %>%
  group_by(ext_processing_run_id, grade_processed) %>%
  summarise(mass = sum(mass, na.rm = TRUE), value = sum(value, na.rm = T)) %>%
  left_join(processing_runs %>% select("__kp_processing_run_id", calc_mass, calc_total_value_dollars, calc_average_price) %>%
              rename("kp_processing_run_id" = "__kp_processing_run_id"), 
            by = c("ext_processing_run_id" = "kp_processing_run_id")) %>%
  rmsfuns::ViewXL()



# UNPROCESSED GRADES


tickets %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
  rename(ext_processing_blend_id = "_ext_processing_blend_id") %>%
  filter(location_current %in% c("Bay 8-9", "TPZ", "Transit - Bay 8-9", "Receiving C", "REC C (TPZ)", " TPZ", "TPZ ")) %>%
  group_by(ext_processing_run_id, ext_processing_blend_id, grade_internal_reclass) %>% 
  summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
  left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  filter(is.na(grade_processed)) %>% rmsfuns::ViewXL()


# ADJUSTING FOR WRONG AUCTION DATA

recalc <- read.delim("clipboard")


tickets2 <-
  tickets1 %>% rename(ext_number_ticket = "_ext_number_ticket") %>%
  left_join(recalc %>% mutate(ext_number_ticket = as.character(ext_number_ticket)), by = "ext_number_ticket") %>%
  mutate(calc_total_value_dollars = coalesce(calc, calc_total_value_dollars))



# SCRAP DESTINATIONS


tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
  rename(ext_processing_blend_id = "_ext_processing_blend_id") %>%
  group_by(ext_processing_run_id, ext_processing_blend_id, grade_internal_reclass) %>% 
  summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
  left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  filter(grepl("SCRAP", grade_processed)) %>%
  rmsfuns::ViewXL()



# FARM STEM DESTINATIONS


tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
  rename(ext_processing_blend_id = "_ext_processing_blend_id") %>%
  filter(grepl("FT", grade_internal_reclass)) %>%
  group_by(ext_processing_run_id, ext_processing_blend_id, grade_internal_reclass) %>% 
  summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
  left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  rmsfuns::ViewXL()



# FARM STEM DESTINATIONS


tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
  rename(ext_processing_blend_id = "_ext_processing_blend_id") %>%
  filter(grepl("/STEM$", grade_internal_reclass)) %>%
  group_by(ext_processing_run_id, ext_processing_blend_id, grade_internal_reclass) %>% 
  summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
  left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  rmsfuns::ViewXL()




# HANDSTRIP EXERCISE


tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
  rename(ext_processing_blend_id = "_ext_processing_blend_id") %>%
  filter(grepl("H$", grade_internal_reclass)) %>%
  left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  filter(grepl("HANDSTRIP", grade_processed)) %>%
  group_by(grade_internal_reclass) %>% 
  summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
  left_join(tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
              filter(grade_internal == "TBDAU" | grade_internal_reclass == "TBDAU") %>% 
              left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
              filter(grepl("/HS$", grade_internal_reclass)) %>% #filter(grepl("H$", grade_processed)) %>%
              group_by(grade_internal_reclass) %>% summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
              mutate(join = gsub("/HS", "", grade_internal_reclass)), by = c("grade_internal_reclass" = "join"), suffix = c("", "_HS")) %>%
  select(grade_internal_reclass, mass, value, grade_internal_reclass_HS, mass_HS, value_HS) %>%
  rmsfuns::ViewXL()



# PICKINGS EXERCISE

tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
  rename(ext_processing_blend_id = "_ext_processing_blend_id") %>%
  filter(grepl("P$", grade_internal_reclass)) %>%
  left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  group_by(grade_processed) %>% 
  summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
  left_join(tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
              filter(grade_internal == "TBDAU" | grade_internal_reclass == "TBDAU") %>% 
              left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
              filter(grepl("/P$", grade_internal_reclass)) %>%
              group_by(grade_processed) %>% summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
              mutate(join = gsub("/P", "", grade_internal_reclass)), by = c("grade_internal_reclass" = "join"), suffix = c("", "_P")) %>%
  select(grade_processed, mass, value, grade_processed_P, mass_P, value_P) %>%
  rmsfuns::ViewXL()



# SCRAP EXERCISE

tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
  rename(ext_processing_blend_id = "_ext_processing_blend_id") %>%
  left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  filter(grepl("SCRAP", grade_processed)) %>%
  group_by(grade_internal_reclass) %>% 
  summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
  left_join(tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
              filter(grade_internal == "TBDAU") %>% 
              left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
              filter(grepl("/S$", grade_internal_reclass)) %>%
              group_by(grade_internal_reclass) %>% summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars, na.rm = TRUE)) %>%
              mutate(join = gsub("/S", "", grade_internal_reclass)), by = c("grade_internal_reclass" = "join"), suffix = c("", "_S")) %>%
  select(grade_internal_reclass, mass, value, grade_internal_reclass_S, mass_S, value_S) %>%
  rmsfuns::ViewXL()


# MTC EXERCISE

tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
  rename(ext_processing_blend_id = "_ext_processing_blend_id") %>%
  left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  filter(grepl("MTC", grade_processed)) %>%
  group_by(grade_internal_reclass) %>% 
  summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
  bind_rows(tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
              filter(grade_internal == "TBDAU") %>% 
              left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
              filter(grepl("MTC", location_current)) %>%
              group_by(grade_internal_reclass) %>% summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars, na.rm = TRUE)) %>%
              mutate(grade_internal_reclass = paste0(grade_internal_reclass, "_MTCR"))) %>%
  rmsfuns::ViewXL()


# NA'S

tickets2 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
  left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  filter(is.na(grade_processed)) %>%
  group_by(grade_internal_reclass, grade_processed) %>% summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
  rmsfuns::ViewXL()



tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
  left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  filter(location_current == "MADOEK")




# DE FACTO FINAL SCRIPT - THE ADDITION OF MISSING TICKETS FROM TPZ



tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
  filter(grade_internal != "TBDAU") %>% rename(ext_processing_blend_id = "_ext_processing_blend_id") %>%
  filter(!grepl("/P$", grade_internal_reclass)) %>%
  filter(location_current %in% c("Bay 8-9", "TPZ", "Transit - Bay 8-9", "Receiving C", "REC C (TPZ)", " TPZ", "TPZ ")) %>%
  group_by(ext_processing_run_id, ext_processing_blend_id) %>% summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
  left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("ext_processing_blend_id"="__kp_processing_blend_id")) %>%
  filter(!grepl("MTC", grade_processed)) %>%
  filter(!grepl("HSPICK", grade_processed)) %>%
  filter(!grepl("SCRAP", grade_processed)) %>%
  filter(!grepl("HANDSTRIP", grade_processed)) %>%
  select(mass, value, grade_processed) %>%
  bind_rows(tickets1 %>% rename(ext_processing_run_id = "_ext_processing_run_id") %>%
              filter(grade_internal == "TBDAU" | grade_internal_reclass == "TBDAU") %>% 
              left_join(processing %>% select("__kp_processing_blend_id", grade_processed), by = c("_ext_processing_blend_id"="__kp_processing_blend_id")) %>%
              filter(!grepl("/P$", grade_internal_reclass)) %>%
              filter(!grepl("HANDSTRIP", grade_processed)) %>%
              filter(!grepl("HSPICK", grade_processed)) %>%
              filter(!grepl("MTC", grade_processed)) %>%
              filter(!grepl("SCRAP", grade_processed)) %>%
              filter(!is.na(grade_processed)) %>%
              group_by(grade_processed) %>% summarise(mass = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars)) %>%
              select(mass, value, grade_processed)) %>%
  ungroup %>% group_by(grade_processed) %>% summarise(mass = sum(mass), value = sum(value)) %>%
  bind_rows(readxl::read_excel("C:/Users/charlesr/Documents/Green Reconciliation/Missing Tickets.xlsx") %>% janitor::clean_names() %>%
              mutate(value = weight*ticket_price) %>% group_by(packed_grade) %>% summarise(mass = sum(weight), value = sum(value)) %>%
              rename(grade_processed = packed_grade)) %>%
  filter(!is.na(grade_processed)) %>%
  group_by(grade_processed) %>% summarise(mass = sum(mass), value = sum(value)) %>%
  rmsfuns::ViewXL()
