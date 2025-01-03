library(tidyverse)
library(odbc)
library(dplyr)
library(DBI)
library(ggplot2)
library(ggalt)
library(openxlsx)




# STC 2023

con23 <- DBI::dbConnect(odbc::odbc(),
                        driver = "FileMaker ODBC",
                        server = "DB02.nt.co.zw", # 127.0.0.1
                        database = "STC 2023.fmp12",
                        uid = "CRandell",
                        pwd = "Ngenile!99")
# SAP connection

sap <- DBI::dbConnect(odbc::odbc(),
                      driver = "HDBODBC",
                      server = "rivdb.riftvalley.com",
                      servernode = "RIVDB:30015",
                      uid = "SYSTEM",
                      pwd = read.table("C:/Users/charlesr/Documents/RSAP git/pass.txt") %>% pull())


# query pulling all 2023 tickets - takes long so i have saved it on local disk

# com <- 
#   DBI::dbGetQuery(con23,  
#   
#     'SELECT
#      date_sale,
#      CASE 
#      WHEN grade_internal_reclass LIKE \'%L\' AND grade_internal_reclass != \'B1L\' 
#      THEN SUBSTR(grade_internal_reclass, 1, LENGTH(grade_internal_reclass) - 1) 
#      ELSE grade_internal_reclass 
#      END AS grade_internal_reclass,
#      CASE 
#      WHEN grade_internal LIKE \'%L\' AND grade_internal != \'B1L\' 
#      THEN SUBSTR(grade_internal, 1, LENGTH(grade_internal) - 1) 
#      ELSE grade_internal 
#      END AS grade_internal, 
#      mass,
#      number_price,
#      calc_total_value_dollars,
#      "_ext_grower_id"
#      FROM tickets T1
#      LEFT JOIN processing_blends T2 ON T2."__kp_processing_blend_id" = T1."_ext_processing_blend_id"
#      WHERE "grade_internal" NOT LIKE \'TBDAU\'
#      AND T2."grade_processed" NOT LIKE \'SHIPMENTS%\'
#      AND "status_tickets" = \'SOLD\' 
#      --FETCH FIRST 100 ROWS ONLY')



com <-
readxl::read_excel("2023 Tickets.xlsx")

grw_est <-
  
  DBI::dbGetQuery(sap, '

SELECT * FROM "NT_DB"."@GROWERESTIMATES1"')


rating <- readxl::read_excel("C:/Users/charlesr/Documents/Ticket Optimiser/Grade Catagories.xlsx") %>% select(-6, -7) %>%
  janitor::clean_names()

crop24 <- read.delim("clipboard") %>% as_tibble() %>% janitor::clean_names() %>% hablar::retype()

staging <-
com %>%
left_join(grw_est %>% select(U_GrowerID, U_TradingName), by = c("_ext_grower_id" = "U_GrowerID")) %>%
  filter(!is.na(U_TradingName)) %>% group_by(grade_internal_reclass) %>% summarise(mass23 = sum(mass)) %>%
  full_join(crop24 %>% select(grade_internal_reclass, crop_throw) %>% group_by(grade_internal_reclass) %>% summarise(mass24 = sum(crop_throw)), 
                                                                                                                     by = "grade_internal_reclass") %>%
  left_join(rating, by = "grade_internal_reclass") %>% filter(!is.na(grade_position)) %>%
  replace(is.na(.), 0)



fun <- function(x) {
    staging %>%
      group_by({{ x }}) %>%
      summarise(mass23 = sum(mass23), mass24 = sum(mass24)) %>%
      arrange(desc(mass23)) %>%
      mutate(mass23 = mass23 / sum(mass23) * 100,
             mass24 = mass24 / sum(mass24) * 100,
             {{ x }} := fct_reorder({{ x }}, mass23))
  }

plot_quality <-
fun(grade_quality) %>%
  ggplot(aes(x = mass23, xend = mass24, y = grade_quality)) +
  geom_dumbbell(
    size = 4,
    dot_guide = TRUE,
    colour = "#a3c4dc",
    colour_xend = "#0e668b",
    dot_guide_size = 0.3,
    dot_guide_colour = "grey60") +
  geom_text(
    aes(label = ifelse(mass24 - mass23 < 0, paste0("(", round(mass24 - mass23, 2), "%)"), 
                       paste0(round(mass24 - mass23, 2), "%"))),
    x = as.numeric(fun(grade_quality) %>% mutate(var = (mass23+mass24)/2) %>% select(4) %>% as_vector()), vjust = -1, size = 3) +
  scale_y_discrete(expand = c(0.1, 0.5)) +
  labs(title = "Grade Quality") + xlab("")

# Rename 'grade_quality' to 'grade_colour'
plot_colour <- fun(grade_colour) %>%
  ggplot(aes(x = mass23, xend = mass24, y = grade_colour)) +
  geom_dumbbell(
    size = 4,
    dot_guide = TRUE,
    colour = "#a3c4dc",
    colour_xend = "#0e668b",
    dot_guide_size = 0.3,
    dot_guide_colour = "grey60") +
  geom_text(
    aes(label = ifelse(mass24 - mass23 < 0, paste0("(", round(mass24 - mass23, 2), "%)"), 
                       paste0(round(mass24 - mass23, 2), "%"))),
    x = as.numeric(fun(grade_colour) %>% mutate(var = (mass23+mass24)/2) %>% select(4) %>% as_vector()), vjust = -1, size = 3) +
  scale_y_discrete(expand = c(0.1, 0.1)) +
  scale_x_continuous(expand = c(0.1, 0.1)) +
  labs(title = "Grade Colour") + xlab("")

# Rename 'grade_colour' back to 'grade_quality'
plot_factor <- 
fun(grade_factor) %>%
  ggplot(aes(x = mass23, xend = mass24, y = grade_factor)) +
  geom_dumbbell(
    size = 4,
    dot_guide = TRUE,
    colour = "#a3c4dc",
    colour_xend = "#0e668b",
    dot_guide_size = 0.3,
    dot_guide_colour = "grey60") +
  geom_text(
    aes(label = ifelse(mass24 - mass23 < 0, paste0("(", round(mass24 - mass23, 2), "%)"), 
                       paste0(round(mass24 - mass23, 2), "%"))),
    x = as.numeric(fun(grade_factor) %>% mutate(var = (mass23+mass24)/2) %>% select(4) %>% as_vector()), vjust = -1, size = 3) +
  scale_y_discrete(expand = c(0.1, 0.1)) +
  scale_x_continuous(expand = c(0.1, 0.1)) + xlab("") + labs(title = "Grade Factor")

plot_position <- 
fun(grade_position) %>%
  ggplot(aes(x = mass23, xend = mass24, y = grade_position)) +
  geom_dumbbell(
    size = 4,
    dot_guide = TRUE,
    colour = "#a3c4dc",
    colour_xend = "#0e668b",
    dot_guide_size = 0.3,
    dot_guide_colour = "grey60") +
  geom_text(
    aes(label = ifelse(mass24 - mass23 < 0, paste0("(", round(mass24 - mass23, 2), "%)"), 
                       paste0(round(mass24 - mass23, 2), "%"))),
    x = as.numeric(fun(grade_position) %>% mutate(var = (mass23+mass24)/2) %>% select(4) %>% as_vector()), vjust = -1, size = 3) +
  scale_y_discrete(expand = c(0.1, 0.1)) +
  scale_x_continuous(expand = c(0.1, 0.1)) + xlab("") + labs(title = "Grade Position")

# Combine all plots
library(gridExtra)
grid.arrange(plot_quality, plot_colour, ncol = 1)
grid.arrange(plot_factor, plot_position, ncol = 1)






tickets23 %>%
  left_join(grw_est %>% select(U_GrowerID, U_TradingName), by = c("_ext_grower_id" = "U_GrowerID")) %>%
  filter(!is.na(U_TradingName)) %>% group_by(grade_internal_reclass) %>% summarise(mass23 = sum(mass), value23 = sum(calc_total_value_dollars)) %>%
  rmsfuns::ViewXL()
  


