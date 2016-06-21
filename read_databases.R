#loading libraries
library(RMySQL)
library(dplyr)
library(DBI)
library(readxl)
library(yaml)
library(readr)

# Get stuff from config
connection_options <- yaml.load_file('credentials.yaml')

# Get all possible tables from all databases
# Go through both the maltem and dss databases to get tables
results_list <- list()
counter <- 0
for (db in c('dssodk', 'maltem', 'maltem_absenteeism')){
  print(db)
  counter <- counter + 1
  # Change the database name
  connection_options$dbname <- db
  # 'dssodk'  #  location, socioecon, housing, individual details
  # maltem'  # MEMBER, HOUSEHOLD, MEMBER_NET, HEALTH_MALARIA_NET, MORTALITY_INFO
  #'#'dss'
  
  # Open connection using dplyr
  con <- do.call('src_mysql', connection_options)
  
  # Tables to read
  tables <- src_tbls(con)
  
  # Make a dataframe of results
  results <- data.frame(table = tables)
  results$db <- db
  results_list[[counter]] <- results
}
results_df <- do.call('rbind', results_list)
# Write a csv of the tables and manually edit
write_csv(results_df, 'tables_to_read.csv')
# Read csv after editing
tables_to_read <- read_csv('tables_to_read_edited.csv')

# Go through both the maltem and dss databases to get tables
for (db in c('dssodk', 'maltem', 'maltem_absenteeism')){
  
  # Change the database name
  connection_options$dbname <- db
    # 'dssodk'  #  location, socioecon, housing, individual details
  # maltem'  # MEMBER, HOUSEHOLD, MEMBER_NET, HEALTH_MALARIA_NET, MORTALITY_INFO
  #'#'dss'
  
  # Open connection using dplyr
  con <- do.call('src_mysql', connection_options)
  
  # Tables to read
  tables <- src_tbls(con)
  
  # Subset the tables to only those I've specified I want
  # to read in
  tables <- tables[tables %in% tables_to_read$table[tables_to_read$db == db]]
  # Read them all in
  for (i in 1:length(tables)){
    table_name <- tables[i]
    message(paste0('Reading from the database: ', table_name))
    try({assign(tables[i],
                tbl(con,
                    tables[i]) %>%
                  collect
                ,
                envir = .GlobalEnv)
      save(table_name, file = 
             paste0('snapshots/',
                    db,
                    '/',
                    Sys.Date(),
                    '_',
                    table_name,
                    '.RData'))
    })
  }
}
rm(results, results_df, tables_to_read)

# Save a snapshot
save.image(paste0('snapshots/',
                  Sys.Date(),
                  '.RData'))
