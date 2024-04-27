# Utility function to log messages with timestamp
log_message <- function(...) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  message(paste0("[", timestamp, "] ", ...))
}

# Source the configuration file
source("rcode/config.R")

# Log a warning message if update_original_avas is TRUE
if (update_original_avas) {
  log_message("WARNING: Script is running in UPDATE MODE. Original AVA GeoJSON files will be overwritten if differences are found.")
}

# Load required libraries
required_packages <- c("sf", "furrr", "future", "jsonlite")

# Install and load missing packages
missing_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
if (length(missing_packages) > 0) {
  log_message(paste("Installing missing packages: ", paste(missing_packages, collapse=", ")))
  install.packages(missing_packages)
}

# Load required packages
library(sf)
library(furrr)
library(future)
library(jsonlite)

# Check temp directory and ensure it exists
if (!dir.exists(temp_dir)) {
  log_message(paste("Creating temporary directory at: ", temp_dir))
  dir.create(temp_dir)
}


# Clean out the avas_updated directory before processing
if (dir.exists(avas_updated_folder)) {
  log_message(paste("Removing existing directory: ", avas_updated_folder))
  unlink(avas_updated_folder, recursive = TRUE)
}
log_message(paste("Creating directory: ", avas_updated_folder))
dir.create(avas_updated_folder)

# Remove the avas.gpkg file if it exists before creating a new one
if (file.exists(geopkg_path)) {
  log_message(paste("Removing existing Geopackage: ", geopkg_path))
  file.remove(geopkg_path)
}
log_message("Starting county shapefile preparation...")

# Check, download, and unzip county shapefile
zip_filename <- basename(county_shapefile_url)
zip_file_path <- file.path(temp_dir, zip_filename)
shapefile_folder_path <- file.path(temp_dir, "shapefile_data")
shapefile_path <- file.path(shapefile_folder_path, sub(".zip$", ".shp", zip_filename))

if (!file.exists(shapefile_path)) {
  if (!file.exists(zip_file_path)) {
    log_message(paste("Downloading county shapefile from: ", county_shapefile_url, " to: ", zip_file_path))
    download.file(county_shapefile_url, zip_file_path)
  }
  log_message(paste("Unzipping county shapefile to: ", shapefile_folder_path))
  unzip(zip_file_path, exdir = shapefile_folder_path)
}

# Load county shapefile
log_message(paste("Loading and transforming county shapefile from: ", shapefile_path))
counties <- st_transform(st_read(shapefile_path, quiet = TRUE), 4326)
ava_files <- head(list.files("./avas", pattern = "*.geojson", full.names = TRUE), n = max_files_to_process)

log_message(paste(length(ava_files), "GeoJSON files found for validation in './avas' directory."))

# Validation Function
validate_geojson_crs <- function(file_path) {
  log_message(paste("START Validating CRS for file: ", file_path))

  ava <- st_read(file_path, quiet = TRUE)
  valid <- !(is.na(st_crs(ava)$epsg) || st_crs(ava)$epsg != 4326)

  log_message(paste("END Validating CRS for file: ", file_path))

  if (!valid) {
    log_message(paste("Warning: Unexpected CRS in ", file_path))
    log_message(paste("Bounding box for ", file_path, ": ", st_bbox(ava)))
  }
  return(list(valid=valid, data=ava))
}

# Validate each GeoJSON file
log_message("Starting validation of GeoJSON files...")
validation_results <- furrr::future_map(ava_files, validate_geojson_crs)
valid_ava_files <- ava_files[sapply(validation_results, `[[`, "valid")]
valid_avas <- lapply(validation_results, `[[`, "data")

log_message(paste(length(valid_ava_files), "GeoJSON files passed validation."))

# Set up the plan to use multiple CPU cores
plan(multisession)

# 1. Store each GeoJSON as a layer in a Geopackage
geopkg_path <- "avas/avas.gpkg"
log_message(paste("Storing GeoJSON files into a Geopackage at: ", geopkg_path))

for(i in seq_along(valid_ava_files)) {
  layer_name <- tools::file_path_sans_ext(basename(valid_ava_files[i]))
  st_write(valid_avas[[i]], geopkg_path, layer = layer_name, append = FALSE, quiet = TRUE)
  log_message(paste("Stored '", layer_name, "' into Geopackage."))
}

# 2. Update the Counties for Each AVA Within the Geopackage
log_message(paste("Updating counties for each AVA within the Geopackage: ", geopkg_path))

layers_in_geopkg <- st_layers(geopkg_path)$name

for(layer_name in layers_in_geopkg) {
  log_message(paste("Processing layer: '", layer_name, "' in Geopackage."))
  ava <- st_read(geopkg_path, layer = layer_name, quiet = TRUE)

  log_message(paste("Running intersection checks for layer: ", layer_name))
  intersection_check <- st_intersects(ava, counties)
  intersected_county_names <- counties$NAME[unlist(intersection_check)]

  if (length(intersected_county_names) > 0) {
    ava$county <- paste(intersected_county_names, collapse = "|")
    st_write(ava, geopkg_path, layer = layer_name, append = FALSE, quiet = TRUE)
    log_message(paste("Updated counties for '", layer_name, "' in the Geopackage."))
  } else {
    log_message(paste("No intersections found for '", layer_name, "'."))
  }
}

# 3. Export Each Layer Back Out as a GeoJSON
log_message(paste("Exporting updated layers from Geopackage to individual GeoJSON files."))

output_folder <- "./avas_updated"
if (!dir.exists(output_folder)) {
  log_message(paste("Creating output directory for updated GeoJSON files at: ", output_folder))
  dir.create(output_folder)
}

for(layer_name in layers_in_geopkg) {
  ava_updated <- st_read(geopkg_path, layer = layer_name, quiet = TRUE)
  geojson_output_path <- paste0(output_folder, "/", layer_name, ".geojson")
  st_write(ava_updated, geojson_output_path, quiet = TRUE)
  log_message(paste("Exported '", layer_name, "' to GeoJSON at: ", geojson_output_path))
}

# Comparison of GeoJSON files
# Directory paths for original and updated GeoJSON files
original_geojson_folder <- "./avas"
updated_geojson_folder <- "./avas_updated"

# List all GeoJSON files in the original and updated folders
original_geojson_files <- list.files(original_geojson_folder, full.names = TRUE, pattern = "\\.geojson$")
if (!is.null(max_files_to_process)) {
  original_geojson_files <- head(original_geojson_files, n = max_files_to_process)
}
updated_geojson_files <- list.files(updated_geojson_folder, pattern = "*.geojson", full.names = TRUE)

# Identify files that changed or are missing in the updated folder
changed_files <- setdiff(original_geojson_files, updated_geojson_files)
added_files <- setdiff(updated_geojson_files, original_geojson_files)


# Identify files that are new in the updated folder
added_files <- setdiff(updated_geojson_files, original_geojson_files)

# Identify files that have changed based on content
changed_files <- list()
common_files <- intersect(basename(original_geojson_files), basename(updated_geojson_files))
for (file in common_files) {
  original_file_path <- file.path(original_geojson_folder, file)
  updated_file_path <- file.path(updated_geojson_folder, file)

  original_content <- jsonlite::fromJSON(original_file_path, simplifyDataFrame = FALSE)
  updated_content <- jsonlite::fromJSON(updated_file_path, simplifyDataFrame = FALSE)

  if (!identical(original_content, updated_content)) {
    changed_files <- append(changed_files, original_file_path)
  }
}

# Log the added and changed files
if (length(added_files) > 0) {
  log_message("The following new GeoJSON files have been added:")
  for (file in added_files) {
    log_message(paste("Added: ", file))
  }
}

if (length(changed_files) > 0) {
  log_message("The following GeoJSON files have changed:")
  for (file in changed_files) {
    log_message(paste("Changed: ", file))
  }
}

log_message("Comparison of GeoJSON files completed.")


# If update_original_avas is TRUE, overwrite the original files with the updated ones where differences are found
if (update_original_avas) {
  for (changed_file in names(changed_files)) {
    updated_file_path <- file.path(avas_updated_folder, paste0(changed_file, ".geojson"))
    original_file_path <- file.path("./avas", paste0(changed_file, ".geojson"))
    file.copy(updated_file_path, original_file_path, overwrite = TRUE)
    log_message(paste("Updated original file with the updated one: ", original_file_path))
  }
}
log_message("Script processing completed!")

# Attribute comparison using jsonlite

# Function to compare attributes of two GeoJSON files
compare_geojson_attributes <- function(original_path, updated_path) {
  original_data <- tryCatch({
    jsonlite::fromJSON(original_path, flatten = TRUE, simplifyDataFrame = FALSE)
  }, error = function() {
    log_message(paste("Error reading original GeoJSON file:", original_path))
    return(NULL)
  })

  updated_data <- tryCatch({
    jsonlite::fromJSON(updated_path, flatten = TRUE, simplifyDataFrame = FALSE)
  }, error = function() {
    log_message(paste("Error reading updated GeoJSON file:", updated_path))
    return(NULL)
  })

  if (is.null(original_data) || is.null(updated_data)) {
    return(NULL)
  }

  original_attributes <- names(original_data$features$properties[[1]])
  updated_attributes <- names(updated_data$features$properties[[1]])

  added_attributes <- setdiff(updated_attributes, original_attributes)
  removed_attributes <- setdiff(original_attributes, updated_attributes)

  list(added = added_attributes, removed = removed_attributes)
}

# Extract the 'features' list from both datasets
original_features <- original_content$features
updated_features <- updated_content$features

# Loop through the features and compare attributes
for (i in seq_along(original_features)) {
  original_attrs <- original_features[[i]]$properties
  updated_attrs <- updated_features[[i]]$properties

  # Loop through attribute names and compare values
  for (attr_name in names(original_attrs)) {
    original_value <- original_attrs[[attr_name]]
    updated_value <- updated_attrs[[attr_name]]

    if (!identical(original_value, updated_value)) {
      log_message("Difference Detected in Feature", i, "- Attribute:", attr_name)
      log_message("Original Value:", original_value)
      log_message("Updated Value:", updated_value)
    }
  }
}


