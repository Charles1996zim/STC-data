library(tidyverse)
library(odbc)
library(dplyr)
library(DBI)
library(janitor)

options("scipen"=10) 



con <- DBI::dbConnect(odbc::odbc(),
                      driver = "FileMaker ODBC",
                      server = "db02.nt.co.zw", # 127.0.0.1
                      database = "STC.fmp12",
                      uid = "CRandell",
                      pwd = "Ngenile!99")


con22 <- DBI::dbConnect(odbc::odbc(),
                      driver = "FileMaker ODBC",
                      server = "db02.nt.co.zw", # 127.0.0.1
                      database = "STC 2022.fmp12",
                      uid = "CRandell",
                      pwd = "Ngenile!99")



## This query gives all the tickets with theri current locations and status in terms of processing


opt <-
  DBI::dbGetQuery(con, 
  
'SELECT grade_internal, grade_position, grade_quality, grade_factor, 
grade_colour, calc_mass_purchased, calc_average_price, calc_average_price_target
FROM grades_internal

WHERE grade_quality <> \'OTHER\'
AND grade_internal NOT LIKE \'%/%\'
                         
                         ')



opt22 <-
  DBI::dbGetQuery(con22, 
                  
'SELECT grade_internal, grade_position, grade_quality, grade_factor, 
grade_colour, calc_mass_purchased, calc_average_price, calc_average_price_target
FROM grades_internal

WHERE grade_quality <> \'OTHER\'
AND grade_internal NOT LIKE \'%/%\'
                         
                         ')



opt %>% group_by(grade_colour) %>% summarise(mass = sum(calc_mass_purchased, na.rm = TRUE)) %>%
  mutate(per = mass/sum(mass)) %>%
  
  left_join(opt22 %>% group_by(grade_colour) %>% summarise(mass = sum(calc_mass_purchased, na.rm = TRUE)) %>%
              mutate(per = mass/sum(mass)), by = "grade_colour") %>% rmsfuns::ViewXL()




opt %>% left_join(opt22 %>% select(grade_internal, calc_mass_purchased), by = "grade_internal") %>%
  group_by(grade_colour) %>% summarise(m23 = sum(calc_mass_purchased.x, na.rm = TRUE), m22 = sum(calc_mass_purchased.y, na.rm = TRUE))




test1 <-
  DBI::dbGetQuery(con, 'SELECT grade_internal, grade_internal_reclass, mass, 
                      calc_total_value_dollars, status_processed, location_current, \"_ext_grower_id\"
  
                      FROM tickets T1

                        WHERE status_tickets = \'SOLD\'
                         
                         ')


test22 <-
  DBI::dbGetQuery(con22, 'SELECT grade_internal, grade_internal_reclass, mass, 
                      calc_total_value_dollars, status_processed, location_current, \"_ext_grower_id\"
  
                      FROM tickets T1

                        WHERE status_tickets = \'SOLD\'
                         
                         ')


test1 %>% as_tibble() %>% rmsfuns::ViewXL()


test1 %>% mutate(location_current = gsub(" ", "", location_current)) %>%
  group_by(grade_internal, grade_internal_reclass, status_processed, `_ext_grower_id`, location_current) %>%
  summarise(mass = sum(mass), calc_total_value_dollars = sum(calc_total_value_dollars)) %>%
  select(1, 2, 6, 7, 3, 5, 4) %>% 
  left_join(
    test1 %>% group_by(`_ext_grower_id`) %>% summarise(mass = sum(mass), value = sum(calc_total_value_dollars)) %>% arrange(desc(mass)) %>%
      mutate(group = ifelse(mass>1000000, "Auciton", ifelse(mass>40000, "Commercial", "Small Scale"))) %>% select(1, 4), by = "_ext_grower_id") %>% 
  ungroup() %>% 
  group_by(grade_internal, grade_internal_reclass, group) %>%
  summarise(mass = sum(mass), calc_total_value_dollars = sum(calc_total_value_dollars)) %>%
  rmsfuns::ViewXL()
# 
# 
# 
# 
# opt22 %>% group_by(grade_internal, grade_position, grade_factor, grade_colour)





var <- "grade_factor"
sel <- "Small Scale"



test1 %>% group_by(`_ext_grower_id`, grade_internal_reclass) %>% summarise(mass = sum(mass), value = sum(calc_total_value_dollars)) %>%
  left_join(
    test1 %>% group_by(`_ext_grower_id`) %>% summarise(mass = sum(mass), value = sum(calc_total_value_dollars)) %>% arrange(desc(mass)) %>%
      mutate(group = ifelse(mass>1000000, "Auciton", ifelse(mass>40000, "Commercial", "Small Scale"))) %>% select(1, 4), by = "_ext_grower_id") %>% 
  mutate(join = gsub("L$", "", grade_internal_reclass)) %>%
  left_join(opt22 %>% select(1, 2, 3, 4, 5), by = c("join" = "grade_internal")) %>%
  left_join(opt22 %>% select(1, 2, 3, 4, 5), by = c("grade_internal_reclass" = "grade_internal"), suffix = c("", "_sec")) %>%
  mutate(grade_position = coalesce(grade_position, grade_position_sec)) %>%
  mutate(grade_quality = coalesce(grade_quality, grade_quality_sec)) %>%
  mutate(grade_colour = coalesce(grade_colour, grade_colour_sec)) %>%
  select(!contains("_sec")) %>% na.omit() %>%
  filter(group == sel) %>%
  mutate(grade_position = gsub("CHINA - ", "", grade_position)) %>%
  mutate(grade_position = gsub("/STRIPS", "", grade_position)) %>%
  mutate(grade_position = gsub("NO POSTION", "NO POSITION", grade_position)) %>%
  group_by_at(var) %>% summarise(sum = sum(mass)) %>% arrange(desc(sum)) %>%
  
  left_join(
    
    test22 %>% group_by(`_ext_grower_id`, grade_internal_reclass) %>% summarise(mass = sum(mass), value = sum(calc_total_value_dollars)) %>%
      left_join(
        test22 %>% group_by(`_ext_grower_id`) %>% summarise(mass = sum(mass), value = sum(calc_total_value_dollars)) %>% arrange(desc(mass)) %>%
          mutate(group = ifelse(mass>1000000, "Auciton", ifelse(mass>40000, "Commercial", "Small Scale"))) %>% select(1, 4), by = "_ext_grower_id") %>% 
      mutate(join = gsub("L$", "", grade_internal_reclass)) %>%
      left_join(opt22 %>% select(1, 2, 3, 4, 5), by = c("join" = "grade_internal")) %>%
      left_join(opt22 %>% select(1, 2, 3, 4, 5), by = c("grade_internal_reclass" = "grade_internal"), suffix = c("", "_sec")) %>%
      mutate(grade_position = coalesce(grade_position, grade_position_sec)) %>%
      mutate(grade_quality = coalesce(grade_quality, grade_quality_sec)) %>%
      mutate(grade_colour = coalesce(grade_colour, grade_colour_sec)) %>%
      select(!contains("_sec")) %>% na.omit() %>%
      filter(group == sel) %>%
      mutate(grade_position = gsub("CHINA - ", "", grade_position)) %>%
      mutate(grade_position = gsub("/STRIPS", "", grade_position)) %>%
      mutate(grade_position = gsub("NO POSTION", "NO POSITION", grade_position)) %>%
      group_by_at(var) %>% summarise(sum = sum(mass)) %>% arrange(desc(sum)), by = var, suffix = c("_23", "_22")
    
  ) %>% mutate(sum_23 = sum_23/sum(sum_23, na.rm = TRUE)) %>%
  mutate(sum_22 = sum_22/sum(sum_22, na.rm = TRUE)) %>% rmsfuns::ViewXL()
              









