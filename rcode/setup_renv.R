# Function to check and install if the package is not installed
check_and_install <- function(pkg_name) {
  if (!requireNamespace(pkg_name, quietly = TRUE)) {
    message(paste("Installing package:", pkg_name))
    renv::install(pkg_name)
  } else {
    message(paste("Package already installed:", pkg_name))
  }
}

# Install renv if not already installed
check_and_install("renv")
check_and_install("codetools")
check_and_install("mgcv")

# Load renv
library(renv)

# Initialize renv for the project (only if it hasn't been initialized already)
if (!file.exists("renv.lock")) {
  renv::init()
}

# List of packages to install
packages <- c(
  "DBI",
  "RPostgreSQL",
  "clipr",
  "codetools",
  "dplyr",
  "furrr",
  "future",
  "geojson",
  "geojsonio",
  "geojsonsf",
  "geosphere",
  "ggplot2",
  "globals",
  "htmlwidgets",
  "httr",
  "jqr",
  "jsonlite",
  "listenv",
  "lwgeom",
  "mgcv",
  "parallelly",
  "plotly",
  "purrr",
  "ragg",
  "raster",
  "readr",
  "rgdal",
  "rvest",
  "sf",
  "sp",
  "stringr",
  "textshaping",
  "tidyverse",
  "vistime",
  "webshot",
  "xml2",
  "zip"
)

# Install packages using renv (only if not already installed)
lapply(packages, check_and_install)

# Update renv.lock. Don't commit this unless you want to update the versions used by everyone.
renv::snapshot()
