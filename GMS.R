library(tidyverse)
library(odbc)
library(dbplyr)
library(DBI)


con <- DBI::dbConnect(odbc::odbc(),
                      driver = "FileMaker ODBC",
                      server = "192.168.8.114", # 127.0.0.1
                      database = "NT GMS.fmp12",
                      uid = "CHARLESR",
                      pwd = "Temp123!")



DBI::dbGetQuery(con, 'SELECT COUNT(*) FROM Accounts Transaction Items')
