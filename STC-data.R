library(odbc)



con <- DBI::dbConnect(odbc::odbc(),
                      driver = "/Library/ODBC/FileMaker ODBC.bundle/Contents/MacOS/FileMaker ODBC",
                      server = "127.0.0.1",
                      database = "/Users/bradcannell/Dropbox/Filemaker Pro/Notes/test_r.fmp12",
                      uid = "Admin",
                      pwd = "password")
