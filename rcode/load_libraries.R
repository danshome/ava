# load_libraries.R

# List of packages required for the project
packages <- c(
    "clipr",
    "sf",
    "codetools",
    "mgcv",
    "dplyr",
    "ggplot2",
    "stringr",
    "lwgeom",
    "tidyverse",
    "rvest",
    "geosphere",
    "sp",
    "raster",
    "rgdal",
    "readr",
    "geojsonio",
    "geojsonsf",
    "xml2",
    "httr",
    "jsonlite",
    "purrr",
    "geojson",
    "zip",
    "vistime",
    "plotly",
    "htmlwidgets",
    "webshot",
    "future",
    "furrr",
    "DBI",
    "RPostgreSQL"
)

# Check if each package is installed, load if it is, or give a warning if not
for (pkg in packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    cat("WARNING: Package", pkg, "is not installed. Consider installing it for full functionality.\n")
  } else {
    cat("Loading package:", pkg, "\n")
    library(pkg, character.only = TRUE)
  }
}
