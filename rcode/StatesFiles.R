# GOAL: separate AVAs by state to make a .geojson and .shp file set for each state

# Load Libraries
library(sf)           # Using sf for spatial operations
library(geojsonsf)
library(geojsonio)   # For geojson operations
library(zip)         # For zipping operations

# Set temp directory to ../.tmp
temp.directory <- ".tmp"

# Check if the temp directory exists, if not, create it
if (!dir.exists(temp.directory)) {
  dir.create(temp.directory)
}

avas <- st_read("./avas_aggregated_files/avas.geojson")

# List of states
states <- c('AR', 'AZ', 'CA', 'CO', 'CT', 'GA', "HI", 'IA', 'ID', 'IL', 'IN', 'KY', 'LA', 'MA', 'MD', 'MI', 'MN', 'MO', 'MS', 'NC', 'NJ', 'NM', 'NY', 'OH', 'OR', 'PA', 'RI', 'TN', 'TX', 'VA', 'WA', 'WI', 'WV')

# Check if FileGDB write support is available
fileGDB_support <- isTRUE(st_drivers()["FileGDB", "write"])

# Loop through states
for (i in 1:length(states)) {
  state.avas <- subset(avas, grepl(states[i], avas$state))
  file.name <- paste(states[i], "avas", sep = "_")
  print(file.name)
  print(c(states[i], dim(state.avas)))

  # Write the geojson file for the state at hand
  geojson_write(state.avas, file = paste("./avas_by_state/", file.name, ".geojson", sep = ""), overwrite = TRUE)

  # Write the shapefile using sf
  st_write(state.avas, dsn = paste0(temp.directory, "/", file.name, ".shp"), driver = "ESRI Shapefile", layer_options = "ENCODING=UTF-8", delete_dsn = TRUE)

  # Write the geopackage using sf. Geopkg is more flexible and doesn't have a 256 char field limitation like shapefiles.
  st_write(state.avas, dsn = paste0(temp.directory, "/", file.name, ".gpkg"), driver = "GPKG", delete_dsn = TRUE)

  # Write File Geodatabase if supported
  if (fileGDB_support) {
    gdb_path <- paste0(temp.directory, "/", states[i], "_avas.gdb")
    st_write(state.avas, dsn = gdb_path, layer = file.name, driver = "FileGDB", layer_options = "ENCODING=UTF-8", delete_dsn = TRUE)
  }

  # Zip the files in the temp directory
  old_wd <- setwd(temp.directory)  # Temporarily set the working directory to the temporary folder

  zip(zipfile = paste(file.name, "_shapefile.zip", sep = ""), files = list.files(pattern = paste0("^", file.name, "\\.")))

  # Remove the shp bits so the next loop starts with an empty folder
  file.remove(list.files(pattern = paste0("^", file.name, "\\.")))

  setwd(old_wd)  # Reset the working directory to the original working directory
}
