# county_shapefile_url: URL to download the county shapefile ZIP archive.
county_shapefile_url <- "https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/COUNTY/tl_rd22_us_county.zip"

# max_files_to_process: Maximum number of GeoJSON files to process.
max_files_to_process <- 300

# temp_dir: Temporary directory path to store downloaded and intermediate files.
temp_dir <- ".tmp"

# Directory where the AVA GeoJSON files are stored
avas_folder <- "./avas"

# Directory to store the updated AVA GeoJSON files
avas_updated_folder <- "./avas_updated"

# update_original_avas: If TRUE, original AVA GeoJSON files will be overwritten with the updated files if differences are found.
update_original_avas <- FALSE

# Path to save the Geopackage file that stores GeoJSON layers during processing.
geopkg_path <- 'avas/avas.gpkg'

postgis_data_directory <- ".tmp/data/directory"
postgis_host <- "localhost"
postgis_port <- 5433
postgis_db <- "avasdb"
postgis_user <- "dmclau"
postgis_password <- "512.633.8086"


