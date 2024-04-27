# Set the global option for future's seed
options(future.seed = TRUE)

# Set a global seed for reproducibility, and RNG kind for parallel processing
set.seed(1L, kind = "L'Ecuyer-CMRG")

library(sf)
library(future)
library(furrr)
library(DBI)
library(jsonlite)
library(RPostgreSQL)

on.exit({
  plan("sequential")  # This will signal to stop any future parallel workers
}, add = TRUE)

# Set the future plan
plan("multisession", workers = 10)

# -----------------------------------------------------------------------------
# Setup and Configuration
# -----------------------------------------------------------------------------

log_message <- function(...) {
  message_string <- paste0("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", ...)
  message(message_string)

  # Open a connection in append mode
  log_connection <- file("ava_county_update_from_shapefile.log", open = "a")

  # Write the message string to the file connection
  writeLines(message_string, log_connection)

  # Close the connection
  close(log_connection)
}

# 1. Function to initialize and start the PostgreSQL server with PostGIS
initialize_postgis_server <- function(data_directory, dbname, host, port, user, password) {
  # First, check if the PostgreSQL server is already running on the specified port
  is_running <- system(paste("pg_isready", "-h", host, "-p", port), intern = TRUE)

  # If the server is running, stop it
  if (grepl("accepting connections", is_running)) {
    message("Stopping the running PostgreSQL server on port ", port)
    system(paste("pg_ctl -D", data_directory, "-o \"-p", port, "\" stop"), intern = FALSE)
    Sys.sleep(5)  # Give some time for the server to stop
  }

  # Ensure the data directory exists and is empty
  if (dir.exists(data_directory)) {
    if (length(list.files(data_directory)) != 0) {
      unlink(data_directory, recursive = TRUE)  # Clear the directory
    }
  } else {
    dir.create(data_directory, recursive = TRUE)  # Create the directory if it does not exist
  }

  # Initialize a data directory
  initdb_cmd <- paste("initdb", data_directory, "--no-locale", "--encoding=UTF8")
  system(initdb_cmd, intern = FALSE, ignore.stdout = FALSE, ignore.stderr = FALSE)

  # Start PostgreSQL on the specified port without waiting for the command to finish
  cmd_start <- paste("pg_ctl -D", shQuote(data_directory), "-o \"-p", shQuote(as.character(port)), "\" start")
  system(cmd_start, intern = FALSE, wait = FALSE)


  # Wait for the PostgreSQL server to start
  Sys.sleep(5)  # Pause to allow the server to start up
  drv <- dbDriver("PostgreSQL")
  for (i in 1:10) {
    Sys.sleep(2)  # Check every 2 seconds
    tryCatch({
      con <- dbConnect(drv, dbname="postgres", host=host, port=port, user=user, password=password)
      if (!is.null(con)) {
        dbDisconnect(con)
        break  # Exit the loop if the connection is successful
      }
    }, error = function(e) {
      if (i == 10) {
        stop("Failed to connect to the database server after multiple attempts. Error: ", e$message)
      } else {
        # Log the error message on failed attempts, except the last one.
        log_message(sprintf("Attempt %d: Failed to connect to the database server. Error: %s", i, e$message))
      }
    })
  }

  # Connect to PostgreSQL to create the database and enable PostGIS
  con <- dbConnect(drv, dbname="postgres", host=host, port=port, user=user, password=password)
  dbExecute(con, sprintf("CREATE DATABASE %s", dbname))
  dbDisconnect(con)

  con <- dbConnect(drv, dbname=dbname, host=host, port=port, user=user, password=password)
  dbExecute(con, "CREATE EXTENSION IF NOT EXISTS postgis;")
  dbDisconnect(con)
}


# 2. Function to import GeoJSON files into PostGIS
import_geojson_to_postgis <- function(files, dbname, host="localhost", port=5433, user="your_user", password="your_password") {
  drv <- dbDriver("PostgreSQL")
  
  plan("sequential") 

  results <- future_map(files, function(file) {
    con <- dbConnect(drv, dbname=dbname, host=host, user=user, password=password, port=port)
    layer_name <- tools::file_path_sans_ext(basename(file))
    layer_name_quoted <- sprintf("\"%s\"", layer_name)  # Ensure the layer name is quoted
    index_name <- sprintf("\"%s_geom_idx\"", layer_name)  # Ensure the index name is quoted

    tryCatch({
      # Read the GeoJSON file into an sf object
      avas <- st_read(file, quiet = TRUE)
      # Now write the sf object to the database
      st_write(avas, con, layer = layer_name, delete_layer = TRUE, quiet = FALSE)
      # Create a spatial index for the new layer, using quoted identifiers
      sql <- sprintf("CREATE INDEX %s ON %s USING GIST (geometry);", index_name, layer_name_quoted)
      dbExecute(con, sql)
      dbDisconnect(con)
      log_message("Successfully imported '", layer_name, "' into PostGIS database with spatial index.")
      return(layer_name)  # return the layer name on success
    }, error = function(e) {
      message("Error writing GeoJSON for layer ", layer_name, ": ", e$message)
      if (!is.null(con) && dbIsValid(con)) {
        dbDisconnect(con)
      }
      log_message("Failed to import '", layer_name, "' into PostGIS database.")
      return(FALSE)  # return FALSE on error
    })
  })

  # Filter out NULL values in case of errors and unlist to get a character vector of layer names
  ava_layers <- unlist(results)
  ava_layers <- ava_layers[sapply(ava_layers, Negate(is.null))]

  return(list(results = results, ava_layers = ava_layers))
}



import_counties_shapefile_to_postgis <- function(shapefile_path, dbname, host="localhost", port=5433, user="your_user", password="your_password") {
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, dbname=dbname, host=host, user=user, password=password, port=port)

  plan("multisession", workers = 10)

  tryCatch({
    counties <- st_read(shapefile_path)  # Read shapefile
    counties <- st_transform(counties, crs = 4326)  # Transform the CRS to EPSG:4326
    st_write(counties, con, layer = "counties", fid_column_name = "GEOID", delete_layer = TRUE, quiet = FALSE)
    # Create a spatial index for the counties layer
    dbExecute(con, "CREATE INDEX counties_geom_idx ON counties USING GIST (geometry);")
    log_message("Successfully imported counties shapefile into PostGIS database with spatial index.")
    TRUE  # return TRUE on success
  }, error = function(e) {
    message("Error writing Shapefile for counties: ", e$message)
    FALSE  # return FALSE on error
  })

  dbDisconnect(con)
  plan("sequential")
}

# Function to quickly filter counties that don't intersect with AVA's at all
filter_relevant_counties <- function(ava, counties) {
  # Find the bounding box of the AVA geometries
  ava_bbox <- st_bbox(ava)

  # Convert the bounding box to an sf polygon
  ava_bbox_polygon <- st_as_sfc(ava_bbox)

  # Perform a bounding-box intersection to find relevant counties
  intersections_matrix <- st_intersects(ava_bbox_polygon, st_geometry(counties), sparse = FALSE)

  # Since the matrix has only one row, we take all columns (counties) that intersect with the AVA bbox
  # The result is a logical vector with a length equal to the number of counties
  potential_counties_index <- intersections_matrix[1, ]

  # Use the logical vector to subset the counties `sf` object
  potential_counties <- counties[potential_counties_index, ]

  return(potential_counties)
}


# Function to read counties from PostGIS and return as an sf object
read_counties_from_postgis <- function(dbname, host, port, user, password) {
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, dbname=dbname, host=host, port=port, user=user, password=password)

  # Assuming the table name in PostGIS is 'counties'
  counties <- st_read(con, "counties")  # Read 'counties' table into sf object

  dbDisconnect(con)  # Always remember to disconnect
  return(counties)
}

read_and_sort_json <- function(file_path) {
  json_data <- fromJSON(file_path, flatten = TRUE)
  return(jsonlite::toJSON(json_data, auto_unbox = TRUE, pretty = TRUE))
}

diff_json_dirs <- function(original_dir, updated_dir) {
  # List files in each directory
  original_files <- list.files(original_dir, pattern = "*.json", full.names = TRUE)
  updated_files <- list.files(updated_dir, pattern = "*.json", full.names = TRUE)

  # Use basename to get filenames without directory path
  original_filenames <- basename(original_files)
  updated_filenames <- basename(updated_files)

  # Loop through each file in the original directory
  for (file_name in original_filenames) {

    original_file_path <- file.path(original_dir, file_name)
    updated_file_path <- file.path(updated_dir, file_name)

    # If the file exists in the updated directory
    if (file_name %in% updated_filenames) {

      original_content <- read_and_sort_json(original_file_path)
      updated_content <- read_and_sort_json(updated_file_path)

      # Write sorted content to temp files for diffing
      original_temp <- tempfile()
      updated_temp <- tempfile()

      writeLines(original_content, original_temp)
      writeLines(updated_content, updated_temp)

      # Use system command to get diff in patch format and print it
      diff_output <- system(paste("diff -u", original_temp, updated_temp), intern = TRUE)
      cat(diff_output, sep = "\n")

      # Remove temp files
      file.remove(original_temp, updated_temp)
    }
  }
}

validate_geojson_crs <- function(file_path) {
  log_message("START Validating CRS for file: ", file_path)
  ava <- st_read(file_path, quiet = TRUE)
  valid <- !(is.na(st_crs(ava)$epsg) || st_crs(ava)$epsg != 4326)
  log_message("END Validating CRS for file: ", file_path)

  if (!valid) {
    log_message("Warning: Unexpected CRS in ", file_path)
    log_message("Bounding box for ", file_path, ": ", st_bbox(ava))
  }
  return(list(valid=valid, data=ava))
}

global_error_handler <- function(e) {
  log_message("An unanticipated error occurred: ", e$message)

  # Print the call stack to the console or log to a file
  log_message("Call stack at the point of error:")
  traceback(2)  # Ignore the call to traceback itself

  # Add any cleanup tasks here if required
  log_message("Script terminated due to an error.")
  stop(e)
}


# Function to calculate intersection areas and filter based on area
calculate_intersections <- function(ava, counties, layer_name) {
  start_time <- Sys.time()
  log_message("Starting calculation of intersecting counties for layer: '", layer_name, "' at ", start_time, ".")

  log_message("Running st_intersection for layer: '", layer_name, "'.")
  int_start_time <- Sys.time()
  int <- st_intersection(ava, counties)
  int_end_time <- Sys.time()
  log_message("Completed st_intersection for layer: '", layer_name, "' in ", int_end_time - int_start_time, " seconds.")

  log_message("Creating data frame for layer: '", layer_name, "'.")
  df_start_time <- Sys.time()
  AVAarea <- data.frame(ava_id = int$ava_id, new_area = as.numeric(st_area(int)), county = int$NAME)
  df_end_time <- Sys.time()
  log_message("Completed data frame for layer: '", layer_name, "' in ", df_end_time - df_start_time, " seconds.")

  log_message("Filtering data frame for layer: '", layer_name, "'.")
  filter_start_time <- Sys.time()
  AVAarea <- AVAarea[AVAarea$new_area >= 1e7, ] # Example threshold: 1x10^7
  filter_end_time <- Sys.time()
  log_message("Completed filtering for layer: '", layer_name, "' in ", filter_end_time - filter_start_time, " seconds.")

  # Log intersecting counties
  if (nrow(AVAarea) > 0) {
    intersecting_counties <- sort(unique(AVAarea$county))
    log_message("Intersecting counties for '", layer_name, "': ", paste(intersecting_counties, collapse = ", "))
  } else {
    log_message("No intersecting counties found for '", layer_name, "'.")
  }

  end_time <- Sys.time()
  log_message("Completed calculating intersecting counties for layer: '", layer_name, "' at ", end_time, ". Total time: ", end_time - start_time, " seconds.")

  return(AVAarea)
}

process_ava_layer <- function(layer_name, postgis_db, postgis_host, postgis_port, postgis_user, postgis_password, counties) {
  log_message("Processing layer: '", layer_name, "' in the database.")

  drv <- dbDriver("PostgreSQL")
  db_conn <- dbConnect(drv, dbname = postgis_db, host = postgis_host, port = postgis_port,
                       user = postgis_user, password = postgis_password)

  # Read the AVA layer from the database
  ava <- st_read(db_conn, layer = layer_name, quiet = TRUE)

  # Pre-filter counties that don't intersect with the AVA at all
  relevant_counties <- filter_relevant_counties(ava, counties)

  # Calculate intersection areas with counties
  AVAarea <- calculate_intersections(ava, relevant_counties, layer_name)

  # Use 'unique' to remove duplicate county names and sort them alphabetically
  intersected_county_names <- sort(unique(AVAarea$county))
  if (length(intersected_county_names) > 0) {
    ava$county <- paste(intersected_county_names, collapse = "|")
    # Update the AVA layer in the database
    st_write(ava, db_conn, layer = layer_name, append = FALSE, quiet = TRUE)
    log_message("Updated sorted counties for '", layer_name, "' in the database.")
  } else {
    log_message("No significant intersections found for '", layer_name, "'.")
  }

  dbDisconnect(db_conn)
}


tryCatch({

  # Source the configuration file
  tryCatch({
    source("rcode/config.R")
  }, error = function(e) {
    log_message("Error loading configuration: ", e$message)
    stop("Terminating script.")
  })

  # Log a warning message if update_original_avas is TRUE
  if (update_original_avas) {
    log_message("WARNING: Script is running in UPDATE MODE. Original AVA GeoJSON files will be overwritten if differences are found.")
  }

  # Load required libraries with error handling
  required_packages <- c("sf", "furrr", "future", "jsonlite","RPostgreSQL")
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

  # -----------------------------------------------------------------------------
  # Prepare Directories and Files
  # -----------------------------------------------------------------------------

  if (!dir.exists(temp_dir)) {
    log_message("Creating temporary directory at: ", temp_dir)
    dir.create(temp_dir)
  }

  # Clean out the avas_updated directory before processing
  if (dir.exists(avas_updated_folder)) {
    log_message("Removing existing directory: ", avas_updated_folder)
    unlink(avas_updated_folder, recursive = TRUE)
  }
  log_message("Creating directory: ", avas_updated_folder)
  dir.create(avas_updated_folder)

  # # Remove the avas.gpkg file if it exists before creating a new one
  # if (file.exists(geopkg_path)) {
  #   log_message("Removing existing Geopackage: ", geopkg_path)
  #   success <- file.remove(geopkg_path)
  #   if (!success) {
  #     log_message("Failed to remove the Geopackage: ", geopkg_path)
  #     stop("Terminating script.")
  #   }
  # } else {
  #   log_message("Geopackage not found, skipping removal: ", geopkg_path)
  # }

  # -----------------------------------------------------------------------------
  # Download and Process County Shapefiles
  # -----------------------------------------------------------------------------

  log_message("Starting county shapefile preparation...")
  zip_filename <- basename(county_shapefile_url)
  zip_file_path <- file.path(temp_dir, zip_filename)
  shapefile_folder_path <- file.path(temp_dir, "shapefile_data")
  shapefile_path <- file.path(shapefile_folder_path, sub(".zip$", ".shp", zip_filename))

  if (!file.exists(shapefile_path)) {
    if (!file.exists(zip_file_path)) {
      log_message("Downloading county shapefile from: ", county_shapefile_url, " to: ", zip_file_path)
      tryCatch({
        download.file(county_shapefile_url, zip_file_path)
      }, error = function(e) {
        log_message("Error downloading shapefile: ", e$message)
        stop("Terminating script.")
      })
    }
    log_message("Unzipping county shapefile to: ", shapefile_folder_path)
    tryCatch({
      unzip(zip_file_path, exdir = shapefile_folder_path)
    }, error = function(e) {
      log_message("Error unzipping shapefile: ", e$message)
      stop("Terminating script.")
    })
  }

  # # Load county shapefile
  # tryCatch({
  #   counties <- st_transform(st_read(shapefile_path, quiet = TRUE), 4326)
  # }, error = function(e) {
  #   log_message("Error transforming shapefile: ", e$message)
  #   stop("Terminating script.")
  # })

  # -----------------------------------------------------------------------------
  # Validation
  # -----------------------------------------------------------------------------

  ava_files <- head(list.files(avas_folder, pattern = "*.geojson", full.names = TRUE), n = max_files_to_process)
  log_message(paste(length(ava_files), "GeoJSON files found for validation in '", avas_folder, "' directory."))

  log_message("Starting validation of GeoJSON files...")
  validation_results <- furrr::future_map(ava_files, validate_geojson_crs)
  valid_ava_files <- ava_files[sapply(validation_results, `[[`, "valid")]
  valid_avas <- lapply(validation_results, `[[`, "data")

  log_message(paste(length(valid_ava_files), "GeoJSON files passed validation."))

  # # -----------------------------------------------------------------------------
  # # GeoJSON Storage
  # # -----------------------------------------------------------------------------
  #
  # log_message("Storing GeoJSON files into a Geopackage at: ", geopkg_path)
  # for(i in seq_along(valid_ava_files)) {
  #   layer_name <- tools::file_path_sans_ext(basename(valid_ava_files[i]))
  #   tryCatch({
  #     st_write(valid_avas[[i]], geopkg_path, layer = layer_name, append = FALSE, quiet = TRUE)
  #   }, error = function(e) {
  #     log_message("Error writing GeoJSON for layer ", layer_name, ": ", e$message)
  #   })
  #   log_message("Stored '", layer_name, "' into Geopackage.")
  # }

  # -----------------------------------------------------------------------------
  # Initialize PostGIS Database
  # -----------------------------------------------------------------------------

  # Assuming 'data_directory', 'dbname', 'port', 'user', and 'password' are defined in your config.R
  initialize_postgis_server(postgis_data_directory, postgis_db, postgis_host, postgis_port, postgis_user, postgis_password)
  log_message("Initialized and started the PostgreSQL server with PostGIS.")

  # -----------------------------------------------------------------------------
  # Import County Shapefile into PostGIS
  # -----------------------------------------------------------------------------

  # Assuming 'shapefile_path' is defined in your config.R
  log_message("Importing County shapefile into PostGIS database.")
  import_counties_shapefile_to_postgis(shapefile_path, postgis_db, postgis_host, postgis_port, postgis_user, postgis_password)
  log_message("County shapefile imported into PostGIS database.")

  # -----------------------------------------------------------------------------
  # Import GeoJSON Files into PostGIS
  # -----------------------------------------------------------------------------

  # Assuming 'valid_ava_files' is a vector of GeoJSON file paths
  log_message("Importing AVA geojson into PostGIS database.")
  import_result <- import_geojson_to_postgis(ava_files, postgis_db, postgis_host, postgis_port, postgis_user, postgis_password)
  results <- import_result$results  # Get the import results
  ava_layers <- import_result$ava_layers  # Get the list of AVA layers


  # -----------------------------------------------------------------------------
  # GeoJSON Update
  # -----------------------------------------------------------------------------

  log_message("Updating counties for each AVA within the database")

  # Before the loop, read counties once since it doesn't change
  counties <- read_counties_from_postgis(postgis_db, postgis_host, postgis_port, postgis_user, postgis_password)

  # Establish a connection to the PostGIS database
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, dbname=postgis_db, host=postgis_host, port=postgis_port, user=postgis_user, password=postgis_password)

  # for (layer_name in ava_layers) {
  #   log_message("Processing layer: '", layer_name, "' in the database.")
  #
  #   # Read the AVA layer from the database
  #   ava <- st_read(paste0("PG:host=", postgis_host, " port=", postgis_port, " dbname=", postgis_db,
  #                         " user=", postgis_user, " password=", postgis_password), layer = layer_name, quiet = TRUE)
  #
  #   # Pre-filter counties that don't intersect with the AVA at all
  #   relevant_counties <- filter_relevant_counties(ava, counties)
  #
  #   # Calculate intersection areas with counties
  #   AVAarea <- calculate_intersections(ava, relevant_counties, layer_name)
  #
  #   # Use 'unique' to remove duplicate county names
  #   intersected_county_names <- unique(AVAarea$county)
  #   log_message("Filtered intersected county names for '", layer_name, "': ", paste(intersected_county_names, collapse = ", "))
  #
  #   if (length(intersected_county_names) > 0) {
  #     # Sort the county names alphabetically
  #     sorted_county_names <- sort(intersected_county_names)
  #     ava$county <- paste(sorted_county_names, collapse = "|")
  #     log_message("Assigned sorted counties for '", layer_name, "': ", ava$county)
  #
  #     # Update the AVA layer in the database
  #     db_conn <- dbConnect(drv, dbname = postgis_db, host = postgis_host, port = postgis_port,
  #                          user = postgis_user, password = postgis_password)
  #     st_write(ava, db_conn, layer = layer_name, append = FALSE, quiet = TRUE)
  #     dbDisconnect(db_conn)
  #
  #     log_message("Updated counties for '", layer_name, "' in the database.")
  #   } else {
  #     log_message("No significant intersections found for '", layer_name, "'.")
  #   }
  # }

  # Set up the future plan for parallel execution
  plan("multisession", workers = 10)  # Adjust the number of workers based on your system's capabilities

  # Execute the process_ava_layer function in parallel over ava_layers
  results <- future_map(ava_layers, ~process_ava_layer(.x, postgis_db, postgis_host, postgis_port, postgis_user, postgis_password, counties))

  # Reset the future plan to sequential after processing
  plan("sequential")


  # -----------------------------------------------------------------------------
  # GeoJSON Export
  # -----------------------------------------------------------------------------

  log_message("Exporting updated layers from PostGIS database to individual GeoJSON files.")
  output_folder <- avas_updated_folder
  if (!dir.exists(output_folder)) {
    log_message("Creating output directory for updated GeoJSON files at: ", output_folder)
    tryCatch({
      dir.create(output_folder)
    }, error = function(e) {
      log_message("Error creating directory ", output_folder, ": ", e$message)
      stop("Terminating script.")
    })
  }

  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, dbname=postgis_db, host=postgis_host, port=postgis_port, user=postgis_user, password=postgis_password)

  for(layer_name in ava_layers) {
    log_message("Exporting layer: '", layer_name, "' to GeoJSON.")
    geojson_output_path <- paste0(output_folder, "/", layer_name, ".geojson")

    # Read the layer from PostGIS
    ava_updated <- st_read(con, layer = layer_name, quiet = TRUE)

    # Write the layer to a GeoJSON file
    st_write(ava_updated, geojson_output_path, driver = "GeoJSON", quiet = TRUE)

    log_message("Exported '", layer_name, "' to GeoJSON at: ", geojson_output_path)
  }

  dbDisconnect(con)

  # -----------------------------------------------------------------------------
  # GeoJSON Comparison
  # -----------------------------------------------------------------------------

  diff_json_dirs(avas_folder, avas_updated_folder)

  log_message("Script processing completed!")

}, error = global_error_handler)

plan("sequential")