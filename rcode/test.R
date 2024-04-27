log_message <- function(...) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  message(paste0("[", timestamp, "] ", ...))
}


# Load required libraries with error handling
required_packages <- c("sf", "furrr", "future", "DBI", "RPostgreSQL", "pool")
missing_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
if (length(missing_packages) > 0) {
  log_message("Installing missing packages: ", paste(missing_packages, collapse=", "))
  for(pkg in missing_packages) {
    tryCatch({
      install.packages(pkg)
    }, error = function(e) {
      log_message("Error installing package ", pkg, ": ", e$message)
      stop("Terminating script.")
    })
  }
}

# Load necessary libraries
library(DBI)
library(RPostgreSQL)
library(sf)
library(furrr)
library(future)
library(pool)


# Source the configuration file
tryCatch({
  source("rcode/config.R")
}, error = function(e) {
  log_message("Error loading configuration: ", e$message)
  stop("Terminating script.")
})

# 1. Function to initialize and start the PostgreSQL server with PostGIS
initialize_postgis_server <- function(data_directory, port) {
  # Initialize a data directory
  system(paste("initdb", data_directory, "> initdb.log 2>&1"))

  # Start PostgreSQL on the specified port and redirect all outputs
  cmd_start <- paste("pg_ctl -D", data_directory, "-o \"-p", port, "\" start > pg_ctl.log 2>&1")
  system(cmd_start)

  # Giving it a few seconds to ensure the server starts
  Sys.sleep(5)
}

# 2. Function to import GeoJSON files into PostGIS
import_geojson_to_postgis <- function(files, host="localhost", port=5433, user="your_user", password="your_password", db="avasdb") {
  drv <- dbDriver("PostgreSQL")

  # This function establishes a connection for each task
  establish_connection <- function(dbname) {
    con <- dbConnect(drv, dbname=dbname, host=host, user=user, password=password, port=port)
    return(con)
  }

  # Connect to the default "postgres" database for setup operations
  con <- establish_connection("postgres")

  # Check if the "avasdb" database exists
  res <- dbGetQuery(con, paste0("SELECT 1 FROM pg_database WHERE datname='", db, "';"))

  # If not, create it
  if (nrow(res) == 0) {
    dbExecute(con, paste0("CREATE DATABASE ", db))
  }

  # Disconnect from the initial setup connection to "postgres"
  dbDisconnect(con)

  # Setup a connection pool for the "avasdb" database
  pool <- dbPool(drv, dbname=db, host=host, user=user, password=password, port=port)

  # Check and create PostGIS extension if it doesn't exist
  con_avasdb <- poolCheckout(pool)
  has_postgis <- dbGetQuery(con_avasdb, "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis');")
  if (!has_postgis$exists[1]) {
    dbExecute(con_avasdb, "CREATE EXTENSION postgis;")
  }
  poolReturn(con_avasdb)

  # Use purrr::map for importing GeoJSON files
  results <- purrr::map(files, function(file) {
    con_task <- poolCheckout(pool)  # Checkout connection from pool

    on.exit(poolReturn(con_task), add = TRUE)  # Ensure that the connection is returned

    # Read the GeoJSON file into an sf object
    geo_data <- sf::st_read(file, quiet=TRUE)

    result <- tryCatch({
      dbBegin(con_task)
      # Use append = TRUE to add to the table, creating it if it doesn't exist
      st_write(geo_data, con_task, table = "all_features", append = TRUE, quiet=FALSE)
      dbCommit(con_task)
      TRUE  # return TRUE on success
    }, error = function(e) {
      dbRollback(con_task)  # Rollback in case of error
      message("Error writing GeoJSON from file ", file, ": ", e$message)
      FALSE  # return FALSE on error
    })

    return(result)
  })

  poolClose(pool)
  return(results)
}

# 3. Function to stop the PostgreSQL server
teardown_postgis_server <- function(data_directory) {
  # Stop the PostgreSQL server
  system(paste("pg_ctl -D", data_directory, "stop"))

  # Remove the data directory
  unlink(data_directory, recursive = TRUE)
}

global_error_handler <- function(e) {
  log_message("An unanticipated error occurred: ", e$message)

  # Add any cleanup tasks here if required
  # Example: Remove temporary directories or files to save space
  log_message("Script terminated due to an error.")
  stop(e)
}

tryCatch({

  log_message("Tearing down temporary PostGIS server...")
  teardown_postgis_server(postgis_data_directory)

  log_message("Initilizaing temporary PostGIS server...")
  initialize_postgis_server(postgis_data_directory,postgis_port)

  log_message("Importing geojson files...")
  ava_files <- head(list.files(avas_folder, pattern = "*.geojson", full.names = TRUE), n = max_files_to_process)
  import_geojson_to_postgis(ava_files, postgis_host, postgis_port, postgis_user, postgis_password, postgis_db)

  # log_message("Tearing down temporary PostGIS server...")
  # teardown_postgis_server(postgis_data_directory)

}, error = global_error_handler)

