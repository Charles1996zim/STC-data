library(tidyverse)
library(odbc)
library(dplyr)
library(DBI)
library(openxlsx)
library(stringr)

options("scipen"=10) 

con20 <- DBI::dbConnect(odbc::odbc(),
                        driver = "FileMaker ODBC",
                        server = "DB02-2022.nt.co.zw", # 127.0.0.1
                        database = "STC 2020.fmp12",
                        uid = "CRandell",
                        pwd = "Ngenile!99")

con22 <- DBI::dbConnect(odbc::odbc(),
                        driver = "FileMaker ODBC",
                        server = "DB02-2022.nt.co.zw", # 127.0.0.1
                        database = "STC 2022.fmp12",
                        uid = "CRandell",
                        pwd = "Ngenile!99")

con23 <- DBI::dbConnect(odbc::odbc(),
                        driver = "FileMaker ODBC",
                        server = "DB02.nt.co.zw", # 127.0.0.1
                        database = "STC 2023.fmp12",
                        uid = "CRandell",
                        pwd = "Ngenile!99")

con24 <- DBI::dbConnect(odbc::odbc(),
                        driver = "FileMaker ODBC",
                        server = "DB02.nt.co.zw", # 127.0.0.1
                        database = "STC.fmp12",
                        uid = "CRandell",
                        pwd = "Ngenile!99")

class22 <-
  DBI::dbGetQuery(con22, "
SELECT grade_internal, grade_position, grade_quality, grade_factor, 
grade_colour, calc_mass_purchased, calc_average_price, calc_average_price_target
FROM grades_internal

WHERE grade_quality <> 'OTHER'
AND grade_internal NOT LIKE '%/%'")

class22 <-
class22 %>% mutate(grade_position = gsub("CHINA - ", "", grade_position)) %>%
  mutate(grade_position = gsub("NO POSTION/STRIPS", "NO POSITION", grade_position)) %>%
  mutate(grade_position = gsub("PRIMINGS", "PRIMING", grade_position)) %>%
  mutate(grade_position = gsub("CUTTERS", "CUTTER", grade_position))


tickets20 <-
  DBI::dbGetQuery(con20, '
SELECT date_sale, grade_internal_reclass, "_ext_grower_id", mass, calc_total_value_dollars
FROM tickets


WHERE status_tickets = \'SOLD\'')



tickets22 <-
  DBI::dbGetQuery(con22, '
SELECT date_sale, grade_internal_reclass, "_ext_grower_id", mass, calc_total_value_dollars
FROM tickets


WHERE status_tickets = \'SOLD\'')


tickets23 <-
  DBI::dbGetQuery(con23, '
SELECT date_sale, grade_internal_reclass, grade_internal, "_ext_grower_id", mass, calc_total_value_dollars
FROM tickets


WHERE status_tickets = \'SOLD\'')



base <-
tickets22 %>% mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                       
                                              ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                              substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                              grade_internal_reclass),
                     
                                              grade_internal_reclass)) %>% rmsfuns::ViewXL()
  
  left_join(class22 %>% select(grade_internal, grade_position, grade_factor, grade_colour), by = c("grade_internal_reclass" = "grade_internal")) %>%
  
  mutate(year = "2022") %>%
  
  bind_rows(
    
    tickets23 %>% mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                                         
                                                         ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                                substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                                grade_internal_reclass),
                                                         
                                                         grade_internal_reclass)) %>%
      
      left_join(class22 %>% select(grade_internal, grade_position, grade_factor, grade_colour), by = c("grade_internal_reclass" = "grade_internal")) %>%
      
      mutate(year = "2023")
    
  )

  
  
base %>% rmsfuns::ViewXL()



sap <- DBI::dbConnect(odbc::odbc(),
                      driver = "HDBODBC",
                      server = "rivdb.riftvalley.com",
                      servernode = "RIVDB:30015",
                      uid = "SYSTEM",
                      pwd = read.table("C:/Users/charlesr/Documents/RSAP git/pass.txt") %>% pull())

grw_est <-

DBI::dbGetQuery(sap, '

SELECT * FROM "NT_DB"."@GROWERESTIMATES1"')


base %>% as_tibble() %>% group_by(grade_internal_reclass, `_ext_grower_id`) %>%
  summarise(mass = sum(mass), value = sum(calc_total_value_dollars)) %>%
  left_join(grw_est %>% select(U_GrowerID, U_TradingName), by = c("_ext_grower_id" = "U_GrowerID")) %>%
  filter(!is.na(U_TradingName)) %>% rmsfuns::ViewXL()



tickets23 %>% as_tibble() %>% left_join(grw_est %>% select(U_GrowerID, U_TradingName), by = c("_ext_grower_id" = "U_GrowerID")) %>%
  filter(!is.na(U_TradingName)) %>%
  group_by(grade_internal_reclass, `_ext_grower_id`) %>% 
  summarise(mass = sum(mass), value = sum(calc_total_value_dollars)) %>%
  mutate(price = value/mass) %>% rmsfuns::ViewXL()



rating <- read.delim("clipboard") %>% as_tibble()


# category setup

setup <-

tickets23 %>%
  
  mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                         
                                         ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                grade_internal_reclass),
                                         
                                         grade_internal_reclass)) %>%
  
  left_join(grw_est %>% select(U_GrowerID, U_TradingName), by = c("_ext_grower_id" = "U_GrowerID")) %>%
  
  filter(!is.na(U_TradingName)) %>%
  
  group_by(grade_internal_reclass, U_TradingName) %>% summarise(mass23 = sum(mass), value = sum(calc_total_value_dollars)) %>%
  
  left_join(rating, by = c("grade_internal_reclass")) %>%
  
  filter(!is.na(Grade.Position)) %>%
  
  select(-value) %>% select(1, 3, 2, everything()) %>%
  
  gather(type, value, 4:7) %>% group_by(type, value, U_TradingName) %>% summarise(mass23 = sum(mass23, na.rm = TRUE)) %>%
  
  left_join(
    
    tickets22 %>%
      
      mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                             
                                             ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                    substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                    grade_internal_reclass),
                                             
                                             grade_internal_reclass)) %>%
      
      left_join(grw_est %>% select(U_GrowerID, U_TradingName), by = c("_ext_grower_id" = "U_GrowerID")) %>%
      
      filter(!is.na(U_TradingName)) %>%
      
      group_by(grade_internal_reclass, U_TradingName) %>% summarise(mass22 = sum(mass), value = sum(calc_total_value_dollars)) %>%
      
      left_join(rating, by = c("grade_internal_reclass")) %>%
      
      filter(!is.na(Grade.Position)) %>%
      
      select(-value) %>% select(1, 3, 2, everything()) %>%
      
      gather(type, value, 4:7) %>% group_by(type, value, U_TradingName) %>% summarise(mass22 = sum(mass22, na.rm = TRUE)), 
    
    by = c("type" = "type", "value" = "value", "U_TradingName" = "U_TradingName")) %>%
  
  left_join(
    
    tickets20 %>%
      
      mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                             
                                             ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                    substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                    grade_internal_reclass),
                                             
                                             grade_internal_reclass)) %>%
      
      left_join(grw_est %>% select(U_GrowerID, U_TradingName), by = c("_ext_grower_id" = "U_GrowerID")) %>%
      
      filter(!is.na(U_TradingName)) %>%
      
      group_by(grade_internal_reclass, U_TradingName) %>% summarise(mass20 = sum(mass), value = sum(calc_total_value_dollars)) %>%
      
      left_join(rating, by = c("grade_internal_reclass")) %>%
      
      filter(!is.na(Grade.Position)) %>%
      
      select(-value) %>% select(1, 3, 2, everything()) %>%
      
      gather(type, value, 4:7) %>% group_by(type, value, U_TradingName) %>% summarise(mass20 = sum(mass20, na.rm = TRUE)), 
    
    by = c("type" = "type", "value" = "value", "U_TradingName" = "U_TradingName"))



  
  
do.call("rbind", replicate(31, grw_est %>% select(U_TradingName), simplify = FALSE)) %>% as_tibble() %>% arrange(U_TradingName) %>%
  bind_cols(do.call("rbind", replicate(85, setup %>% distinct(type) %>% ungroup(), simplify = FALSE))) %>% 
  left_join(setup, by = c("type" = "type", "value" = "value", "U_TradingName" = "U_TradingName")) %>% 
  select(1, 3, 2, 6, 5, 4) %>% mutate(type = gsub("\\.", " ", type)) %>% replace(is.na(.), 0) %>%
  rmsfuns::ViewXL()



# grade by grade setup



tickets23 %>%
  
  mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                         
                                         ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                grade_internal_reclass),
                                         
                                         grade_internal_reclass)) %>%
  
  left_join(grw_est %>% select(U_GrowerID, U_TradingName), by = c("_ext_grower_id" = "U_GrowerID")) %>%
  
  filter(!is.na(U_TradingName)) %>%
  
  group_by(grade_internal_reclass, U_TradingName) %>% summarise(mass23 = sum(mass), value = sum(calc_total_value_dollars)) %>%
  
  left_join(rating, by = c("grade_internal_reclass")) %>%
  
  filter(!is.na(Grade.Position)) %>%
  
  select(-value) %>% select(1, 3, 2, everything()) %>%
  
  group_by(U_TradingName, grade_internal_reclass) %>% summarise(mass23 = sum(mass23, na.rm = TRUE)) %>%
  
  full_join(
    
    tickets22 %>%
      
      mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                             
                                             ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                    substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                    grade_internal_reclass),
                                             
                                             grade_internal_reclass)) %>%
      
      left_join(grw_est %>% select(U_GrowerID, U_TradingName), by = c("_ext_grower_id" = "U_GrowerID")) %>%
      
      filter(!is.na(U_TradingName)) %>%
      
      group_by(grade_internal_reclass, U_TradingName) %>% summarise(mass22 = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars, na.rm = TRUE)) %>%
      
      left_join(rating, by = c("grade_internal_reclass")) %>%
      
      filter(!is.na(Grade.Position)) %>%
      
      select(-value) %>% select(1, 3, 2, everything()) %>%
      
      group_by(U_TradingName, grade_internal_reclass) %>% summarise(mass22 = sum(mass22, na.rm = TRUE)), 
    
    by = c("U_TradingName" = "U_TradingName", "grade_internal_reclass" = "grade_internal_reclass")) %>% 
  
  
  # Remove Lose leaf
  
  mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                         
                                         ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                grade_internal_reclass),
                                         
                                         grade_internal_reclass)) %>%
  
  group_by(U_TradingName, grade_internal_reclass) %>% summarise(mass22 = sum(mass22, na.rm = TRUE), mass23 = sum(mass23, na.rm = TRUE)) %>%
  
  rmsfuns::ViewXL()





  # Pricing matrix    
    
  
  
  
tickets23 %>%
  
  left_join(class22 %>% select(grade_internal, grade_position, grade_factor, grade_colour), by = c("grade_internal_reclass" = "grade_internal")) %>%
  
  left_join(grw_est %>% select(U_GrowerID, U_TradingName), by = c("_ext_grower_id" = "U_GrowerID")) %>%
  
  filter(!is.na(U_TradingName)) %>%
  
  group_by(grade_internal_reclass, U_TradingName) %>% summarise(mass23 = sum(mass), value = sum(calc_total_value_dollars)) %>%
  
  left_join(rating, by = c("grade_internal_reclass")) %>%
  
  filter(!is.na(grade_position)) %>%
  
  select(1, 3, 2, everything()) %>%
  
  group_by(U_TradingName, grade_internal_reclass) %>% summarise(mass23 = sum(mass23, na.rm = TRUE), value23 = sum(value, na.rm = TRUE)) %>%
  
  full_join(
    
    tickets22 %>%
      
      left_join(class22 %>% select(grade_internal, grade_position, grade_factor, grade_colour), by = c("grade_internal_reclass" = "grade_internal")) %>%
      
      left_join(grw_est %>% select(U_GrowerID, U_TradingName), by = c("_ext_grower_id" = "U_GrowerID")) %>%
      
      filter(!is.na(U_TradingName)) %>%
      
      group_by(grade_internal_reclass, U_TradingName) %>% summarise(mass22 = sum(mass, na.rm = TRUE), value = sum(calc_total_value_dollars, na.rm = TRUE)) %>%
      
      left_join(rating, by = c("grade_internal_reclass")) %>%
      
      filter(!is.na(grade_position)) %>%
      
      select(1, 3, 2, everything()) %>%
      
      group_by(U_TradingName, grade_internal_reclass) %>% summarise(mass22 = sum(mass22, na.rm = TRUE), value22 = sum(value, na.rm = TRUE)), 
    
    by = c("U_TradingName" = "U_TradingName", "grade_internal_reclass" = "grade_internal_reclass")) %>% 
  
  
  # Remove Lose leaf
  
  mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                         
                                         ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                grade_internal_reclass),
                                         
                                         grade_internal_reclass)) %>%
  
  group_by(grade_internal_reclass) %>% summarise(mass22 = sum(mass22, na.rm = TRUE), mass23 = sum(mass23, na.rm = TRUE),
                                                                value23 = sum(value23, na.rm = TRUE), value22 = sum(value22, na.rm = TRUE)) %>%
  
  rmsfuns::ViewXL()
  




## SMALL SCALE CROP





# rating <- read.delim("clipboard") %>% as_tibble()


region <- read.delim("clipboard") %>% as_tibble() %>% clean_names()


# category setup

setupss <-
  
  tickets23 %>%
  
  mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                         
                                         ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                grade_internal_reclass),
                                         
                                         grade_internal_reclass)) %>%
  
  left_join(region, by = c("_ext_grower_id" = "grw_number")) %>%
  
  filter(!is.na(region)) %>%
  
  group_by(grade_internal_reclass, region) %>% summarise(mass23 = sum(mass), value = sum(calc_total_value_dollars)) %>%
  
  left_join(rating, by = c("grade_internal_reclass")) %>%
  
  filter(!is.na(Grade.Position)) %>%
  
  select(-value) %>% select(1, 3, 2, everything()) %>%
  
  gather(type, value, 4:7) %>% group_by(type, value, region) %>% summarise(mass23 = sum(mass23, na.rm = TRUE)) %>%
  
  left_join(
    
    tickets22 %>%
      
      mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                             
                                             ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                    substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                    grade_internal_reclass),
                                             
                                             grade_internal_reclass)) %>%
      
      left_join(region, by = c("_ext_grower_id" = "grw_number")) %>%
      
      filter(!is.na(region)) %>%
      
      group_by(grade_internal_reclass, region) %>% summarise(mass22 = sum(mass), value = sum(calc_total_value_dollars)) %>%
      
      left_join(rating, by = c("grade_internal_reclass")) %>%
      
      filter(!is.na(Grade.Position)) %>%
      
      select(-value) %>% select(1, 3, 2, everything()) %>%
      
      gather(type, value, 4:7) %>% group_by(type, value, region) %>% summarise(mass22 = sum(mass22, na.rm = TRUE)), 
    
    by = c("type" = "type", "value" = "value", "region" = "region")) %>%
  
  left_join(
    
    tickets20 %>%
      
      mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                             
                                             ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                    substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                    grade_internal_reclass),
                                             
                                             grade_internal_reclass)) %>%
      
      left_join(region, by = c("_ext_grower_id" = "grw_number")) %>%
      
      filter(!is.na(region)) %>%
      
      group_by(grade_internal_reclass, region) %>% summarise(mass20 = sum(mass), value = sum(calc_total_value_dollars)) %>%
      
      left_join(rating, by = c("grade_internal_reclass")) %>%
      
      filter(!is.na(Grade.Position)) %>%
      
      select(-value) %>% select(1, 3, 2, everything()) %>%
      
      gather(type, value, 4:7) %>% group_by(type, value, region) %>% summarise(mass20 = sum(mass20, na.rm = TRUE)), 
    
    by = c("type" = "type", "value" = "value", "region" = "region"))




setupss %>%   select(3, 1, 2, 6, 5, 4) %>% mutate(type = gsub("\\.", " ", type)) %>% replace(is.na(.), 0) %>%
  rmsfuns::ViewXL()




# Grade by Grade Setup



tickets23 %>%
  
  left_join(class22 %>% select(grade_internal, grade_position, grade_factor, grade_colour), by = c("grade_internal_reclass" = "grade_internal")) %>%
  
  left_join(region, by = c("_ext_grower_id" = "grw_number")) %>%
  
  filter(!is.na(region)) %>%
  
  group_by(grade_internal_reclass, region) %>% summarise(mass23 = sum(mass), value = sum(calc_total_value_dollars)) %>%
  
  left_join(rating, by = c("grade_internal_reclass")) %>%
  
  filter(!is.na(grade_position)) %>%
  
  select(-value) %>% select(1, 3, 2, everything()) %>%
  
  group_by(region, grade_internal_reclass) %>% summarise(mass23 = sum(mass23, na.rm = TRUE)) %>%
  
  full_join(
    
    tickets22 %>%
      
      left_join(class22 %>% select(grade_internal, grade_position, grade_factor, grade_colour), by = c("grade_internal_reclass" = "grade_internal")) %>%
      
      left_join(region, by = c("_ext_grower_id" = "grw_number")) %>%
      
      filter(!is.na(region)) %>%
      
      group_by(grade_internal_reclass, region) %>% summarise(mass22 = sum(mass), value = sum(calc_total_value_dollars)) %>%
      
      left_join(rating, by = c("grade_internal_reclass")) %>%
      
      filter(!is.na(grade_position)) %>%
      
      select(-value) %>% select(1, 3, 2, everything()) %>%
      
      group_by(region, grade_internal_reclass) %>% summarise(mass22 = sum(mass22, na.rm = TRUE)), 
    
    by = c("region" = "region", "grade_internal_reclass" = "grade_internal_reclass")) %>% 
  
  
  # Remove Lose leaf
  
  mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                         
                                         ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                grade_internal_reclass),
                                         
                                         grade_internal_reclass)) %>%
  
  group_by(region, grade_internal_reclass) %>% summarise(mass22 = sum(mass22, na.rm = TRUE), mass23 = sum(mass23, na.rm = TRUE)) %>%
  
  rmsfuns::ViewXL()






# Pricing matrix    




tickets23 %>%
  
  left_join(region, by = c("_ext_grower_id" = "grw_number")) %>%
  
  filter(!is.na(region)) %>%
  
  group_by(grade_internal_reclass, region) %>% summarise(mass23 = sum(mass), value = sum(calc_total_value_dollars)) %>%
  
  left_join(rating, by = c("grade_internal_reclass")) %>%
  
  filter(!is.na(grade_position)) %>%
  
  select(1, 3, 2, everything()) %>%
  
  group_by(region, grade_internal_reclass) %>% summarise(mass23 = sum(mass23, na.rm = TRUE), value23 = sum(value, na.rm = TRUE)) %>%
  
  full_join(
    
    tickets22 %>%
      
      left_join(region, by = c("_ext_grower_id" = "grw_number")) %>%
      
      filter(!is.na(region)) %>%
      
      group_by(grade_internal_reclass, region) %>% summarise(mass22 = sum(mass), value = sum(calc_total_value_dollars)) %>%
      
      left_join(rating, by = c("grade_internal_reclass")) %>%
      
      filter(!is.na(grade_position)) %>%
      
      select(1, 3, 2, everything()) %>%
      
      group_by(region, grade_internal_reclass) %>% summarise(mass22 = sum(mass22, na.rm = TRUE), value22 = sum(value, na.rm = TRUE)), 
    
    by = c("region" = "region", "grade_internal_reclass" = "grade_internal_reclass")) %>% 
  
  
  # Remove Lose leaf
  
  mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                         
                                         ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                grade_internal_reclass),
                                         
                                         grade_internal_reclass)) %>%
  
  group_by(grade_internal_reclass) %>% summarise(mass22 = sum(mass22, na.rm = TRUE), mass23 = sum(mass23, na.rm = TRUE),
                                                 value23 = sum(value23, na.rm = TRUE), value22 = sum(value22, na.rm = TRUE)) %>%
  
  rmsfuns::ViewXL()




# loss of china grades



grade_match <- read.delim("clipboard") %>% as_tibble()



tickets23 %>%
  
  mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                         
                                         ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                grade_internal_reclass),
                                         
                                         grade_internal_reclass)) %>%
  
  left_join(grw_est %>% select(U_GrowerID, U_TradingName), by = c("_ext_grower_id" = "U_GrowerID")) %>%
  
  filter(!is.na(U_TradingName)) %>%
  
  group_by(grade_internal_reclass, U_TradingName) %>% summarise(mass23 = sum(mass), value = sum(calc_total_value_dollars)) %>%
  
  left_join(grade_match %>% select(grade_internal, Grade.Match), by = c("grade_internal_reclass" = "grade_internal")) %>%
  
  filter(!is.na(Grade.Match)) %>%
  
  filter(U_TradingName %in% c("Argosy Farm (Pvt) Ltd", "California Estate (Pvt) Ltd", "Dovegrove Trading (Pvt) Ltd", "Gakumi Investments (Pvt) Ltd",
                              "Gillanders Investments (Pvt) Ltd", "H A Blignaut (Pvt) Ltd", "Kurima Farm (Pvt) Ltd t/a Brundle",
                              "Mumvee Investments (Pvt) Ltd", "Nhimbe Fresh Exports (Pvt) Ltd")) %>%
  
  group_by(Grade.Match) %>% summarise(mass = sum(mass23)) %>% arrange(desc(mass)) %>% write.xlsx("C:/Users/charlesr/Documents/Ticket Optimiser/Lost Blends.xlsx")




rerun <-
read.delim("clipboard") %>% as_tibble()

rerun1 <-
rerun %>% select(1,3, 4, 5, 6, 8, 9, 10) %>%
  gather(type, value, 2:5) %>% group_by(Grower.ID, type, value) %>%
  summarise(mass21 = sum(X2021), mass22 = sum(X2022), mass23 = sum(X2023)) %>% ungroup() %>%
  group_by(type, value)




do.call("rbind", replicate(31, grw_est %>% select(U_TradingName), simplify = FALSE)) %>% as_tibble() %>% arrange(U_TradingName) %>%
  bind_cols(do.call("rbind", replicate(85, rerun1 %>% distinct(type) %>% ungroup(), simplify = FALSE))) %>% 
  left_join(rerun1, by = c("type" = "type", "value" = "value", "U_TradingName" = "Grower.ID")) %>% 
  select(1, 3, 2, 4, 5, 6) %>% mutate(type = gsub("_", " ", type)) %>% mutate(type = str_to_title(type)) %>%
  replace(is.na(.), 0) %>%
  rmsfuns::ViewXL()





tickets23 %>% mutate(grade_internal_reclass = ifelse(grade_internal_reclass != "B1L",
                                                     
                                                     ifelse(substr(grade_internal_reclass, nchar(grade_internal_reclass), nchar(grade_internal_reclass)) == "L",
                                                            substr(grade_internal_reclass, 1, nchar(grade_internal_reclass) - 1),
                                                            grade_internal_reclass),
                                                     
                                                     grade_internal_reclass)) %>%
  group_by(date_sale, grade_internal_reclass, `_ext_grower_id`) %>% summarise(mass = sum(mass), value = sum(calc_total_value_dollars)) %>%
  
  rmsfuns::ViewXL()
