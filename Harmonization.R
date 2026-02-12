#Download packages
suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(janitor)
  library(writexl)
  # library(wbstats) # NOT used (WB API has been 502'ing)
})

#Settings
start_year <- 2000
end_year   <- 2025

in_dir  <- "Outputs"
out_dir <- "Outputs"
dir.create(out_dir, showWarnings = FALSE)

themes <- c("health", "poverty", "education", "gender", "agriculture", "aid")

# -----------------------------
# OFFLINE LAC ISO3 list (no WB API calls)
# Includes WB LAC countries + territories.
# If you want sovereign-only instead, I can swap this list.
# -----------------------------
latam_iso3 <- c(
  "ABW","ARG","ATG","BHS","BLZ","BOL","BRA","BRB","CHL","COL","CRI","CUB","CUW",
  "CYM","DMA","DOM","ECU","GRD","GTM","GUY","HND","HTI","JAM","KNA","LCA","MAF",
  "MEX","NIC","PAN","PER","PRI","PRY","SLV","SUR","SXM","TCA","TTO","URY","VCT",
  "VEN","VGB","VIR"
)

# -----------------------------
# Function: harmonize one theme
# -----------------------------
harmonize_theme <- function(theme) {
  
  in_path <- file.path(in_dir, paste0(theme, "_wide.csv"))
  stopifnot(file.exists(in_path))
  
  message("\n==============================")
  message("HARMONIZING THEME: ", theme)
  message("==============================")
  
  #Load wide-format data
  panel_raw <- readr::read_csv(in_path, show_col_types = FALSE) %>%
    janitor::clean_names()
  
  # ---- Convert WB date/year robustly (avoid year() closure issues) ----
  if ("year" %in% names(panel_raw)) {
    panel_raw <- panel_raw %>% mutate(year = suppressWarnings(as.numeric(.data$year)))
  } else if ("date" %in% names(panel_raw)) {
    panel_raw <- panel_raw %>% mutate(year = suppressWarnings(as.numeric(.data$date)))
  } else {
    stop("Could not find a 'date' or 'year' column in ", in_path)
  }
  
  # Standardize iso3c
  panel_raw <- panel_raw %>%
    mutate(
      iso3c = toupper(str_trim(as.character(iso3c)))
    )
  
  required_cols <- c("iso3c", "country", "year")
  missing_cols <- setdiff(required_cols, names(panel_raw))
  if (length(missing_cols) > 0) {
    stop(paste("Missing required columns:", paste(missing_cols, collapse = ", ")))
  }
  
  #Ensure only data from the years window
  panel_raw <- panel_raw %>%
    filter(year >= start_year & year <= end_year)
  
  #Filter to LAC
  panel_latam <- panel_raw %>%
    mutate(iso3c = toupper(str_trim(as.character(iso3c)))) %>%
    filter(iso3c %in% latam_iso3)
  
  message("LAC countries in file: ", length(unique(panel_latam$iso3c)))
  
  #Identify indicator columns
  id_cols <- intersect(c("iso3c", "iso2c", "country", "year"), names(panel_latam))
  ind_cols <- setdiff(names(panel_latam), c(id_cols, "date", "region", "adminregion", "income_level"))
  
  #Make sure indicator names are unique (drop duplicate names by keeping the first)
  dup_name_flags <- duplicated(ind_cols)
  if (any(dup_name_flags)) {
    message("Found duplicate indicator column NAMES after cleaning. Keeping the first instance")
    ind_cols_unique <- ind_cols[!dup_name_flags]
    panel_latam <- panel_latam %>% select(all_of(id_cols), all_of(ind_cols_unique))
    ind_cols <- ind_cols_unique
  }
  
  #----Ensure complete country-year grid before applying the missingness filter----
  id_cols_panel <- intersect(c("iso3c", "iso2c", "country", "year"), names(panel_latam))
  id_cols_country <- intersect(c("iso3c", "iso2c", "country"), names(panel_latam))
  
  grid <- expand.grid(
    iso3c = sort(unique(panel_latam$iso3c)),
    year  = seq(start_year, end_year),
    stringsAsFactors = FALSE
  ) %>%
    as_tibble() %>%
    left_join(
      panel_latam %>% select(all_of(id_cols_country)) %>% distinct(),
      by = "iso3c"
    ) %>%
    select(iso3c, iso2c, country, year)
  
  panel_complete <- grid %>%
    left_join(
      panel_latam %>% select(all_of(id_cols_panel), all_of(ind_cols)),
      by = c("iso3c", "iso2c", "country", "year")
    )
  
  message(
    "Balanced rows: ", nrow(panel_complete),
    " (", length(unique(panel_complete$iso3c)), " countries x ",
    length(unique(panel_complete$year)), " years)"
  )
  
  #Drop indicators with >50% missingness
  miss_rate <- sapply(panel_complete[ind_cols], function(x) mean(is.na(x)))
  keep_inds <- names(miss_rate)[miss_rate <= 0.50]
  
  id_cols2 <- intersect(c("iso3c", "iso2c", "country", "year"), names(panel_complete))
  panel_kept <- panel_complete %>% select(all_of(id_cols2), all_of(keep_inds))
  
  message("Indicators before missingness filter: ", length(ind_cols))
  message("Indicators kept(<=50% missing): ", length(keep_inds))
  
  #----Remove redundant indicators (identical columns)----
  ind2 <- setdiff(names(panel_kept), id_cols2)
  
  col_sig <- sapply(panel_kept[ind2], function(x) {
    paste0(ifelse(is.na(x), "NA", format(x, scientific = FALSE, digits = 15)), collapse = "|")
  })
  
  drop_identical <- duplicated(col_sig)
  if (any(drop_identical)) {
    message("Dropping ", sum(drop_identical), " redundant (identical) indicators")
  }
  
  panel_final <- panel_kept %>%
    select(all_of(id_cols2), all_of(ind2[!drop_identical]))
  
  final_indicators <- setdiff(names(panel_final), id_cols2)
  message("Final indicator count after redundancy drop: ", length(final_indicators))
  
  latest_country <- panel_final %>%
    arrange(iso3c, desc(year)) %>%
    group_by(iso3c, iso2c, country) %>%
    summarize(
      across(all_of(final_indicators),
             ~ { v <- .x[!is.na(.x)]; if (length(v)) v[1] else NA_real_ }),
      .groups = "drop"
    ) %>%
    arrange(country)
  
  #----Save outputs----
  panel_csv  <- file.path(out_dir, paste0(theme, "_lac_panel_2000_2025.csv"))
  panel_xlsx <- file.path(out_dir, paste0(theme, "_lac_panel_2000_2025.xlsx"))
  
  latest_csv  <- file.path(out_dir, paste0(theme, "_lac_latest_country_wide.csv"))
  latest_xlsx <- file.path(out_dir, paste0(theme, "_lac_latest_country_wide.xlsx"))
  
  write_csv(panel_final, panel_csv)
  write_xlsx(list(panel_2000_2025 = panel_final), panel_xlsx)
  
  write_csv(latest_country, latest_csv)
  write_xlsx(list(latest_country_wide = latest_country), latest_xlsx)
  
  cat("\nDONE  ", theme, "\nSaved:\n",
      " - ", normalizePath(panel_csv,  winslash="/"), "\n",
      " - ", normalizePath(panel_xlsx, winslash="/"), "\n",
      " - ", normalizePath(latest_csv,  winslash="/"), "\n",
      " - ", normalizePath(latest_xlsx, winslash="/"), "\n", sep = "")
  
  invisible(TRUE)
}

# -----------------------------
# Run all themes
# -----------------------------
invisible(lapply(themes, harmonize_theme))

cat("\nALL THEMES COMPLETE \n")