# Harmonize World Bank wide files for Latin America & Caribbean (2000–2025)
# - keeps a complete country-year grid
# - drops indicators with >50% missingness over that grid
# - outputs a panel file + a “latest value” wide file for each theme

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(janitor)
  library(writexl)
})

start_year <- 2000
end_year   <- 2025

in_dir  <- "Outputs"
out_dir <- "Outputs"
dir.create(out_dir, showWarnings = FALSE)

themes <- c("health", "poverty", "education", "gender", "agriculture", "aid")

# LAC iso3 codes (includes territories used by World Bank regional grouping)
lac_iso3 <- c(
  "ABW","ARG","ATG","BHS","BLZ","BOL","BRA","BRB","CHL","COL","CRI","CUB","CUW",
  "CYM","DMA","DOM","ECU","GRD","GTM","GUY","HND","HTI","JAM","KNA","LCA","MAF",
  "MEX","NIC","PAN","PER","PRI","PRY","SLV","SUR","SXM","TCA","TTO","URY","VCT",
  "VEN","VGB","VIR"
)

harmonize_one <- function(theme) {
  in_path <- file.path(in_dir, paste0(theme, "_wide.csv"))
  if (!file.exists(in_path)) stop("Missing input file: ", in_path)
  
  message("\n--- ", theme, " ---")
  
  df <- read_csv(in_path, show_col_types = FALSE) %>%
    clean_names()
  
  # year/date handling (avoid any year() name collisions)
  if ("year" %in% names(df)) {
    df <- df %>% mutate(year = suppressWarnings(as.integer(.data$year)))
  } else if ("date" %in% names(df)) {
    df <- df %>% mutate(year = suppressWarnings(as.integer(.data$date)))
  } else {
    stop("No 'year' or 'date' column found in: ", in_path)
  }
  
  df <- df %>%
    mutate(
      iso3c = toupper(str_trim(as.character(iso3c))),
      country = as.character(country)
    ) %>%
    filter(year >= start_year, year <= end_year) %>%
    filter(iso3c %in% lac_iso3)
  
  message("LAC countries: ", n_distinct(df$iso3c))
  
  id_cols  <- intersect(c("iso3c", "iso2c", "country", "year"), names(df))
  ind_cols <- setdiff(names(df), c(id_cols, "date", "region", "adminregion", "income_level"))
  
  # drop duplicated indicator names (keep first)
  if (any(duplicated(ind_cols))) {
    ind_cols <- ind_cols[!duplicated(ind_cols)]
    df <- df %>% select(all_of(id_cols), all_of(ind_cols))
  }
  
  # build full country-year grid first
  ids_country <- intersect(c("iso3c", "iso2c", "country"), names(df))
  ids_panel   <- intersect(c("iso3c", "iso2c", "country", "year"), names(df))
  
  grid <- tidyr::expand_grid(
    iso3c = sort(unique(df$iso3c)),
    year  = seq(start_year, end_year)
  ) %>%
    left_join(df %>% select(all_of(ids_country)) %>% distinct(), by = "iso3c") %>%
    select(any_of(c("iso3c", "iso2c", "country")), year)
  
  complete <- grid %>%
    left_join(df %>% select(all_of(ids_panel), all_of(ind_cols)),
              by = intersect(c("iso3c", "iso2c", "country", "year"), names(grid)))
  
  message("Rows (balanced): ", nrow(complete),
          " = ", n_distinct(complete$iso3c), " x ", n_distinct(complete$year))
  
  # drop indicators with >50% missing over balanced grid
  miss_rate <- sapply(complete[ind_cols], function(x) mean(is.na(x)))
  keep_inds <- names(miss_rate)[miss_rate <= 0.50]
  
  message("Indicators: ", length(ind_cols), " -> ", length(keep_inds), " kept")
  
  kept <- complete %>% select(all_of(id_cols), all_of(keep_inds))
  
  # drop identical indicator columns (rare but possible)
  ind2 <- setdiff(names(kept), id_cols)
  sig <- sapply(kept[ind2], function(x) paste(ifelse(is.na(x), "NA", format(x, digits = 15)), collapse = "|"))
  kept <- kept %>% select(all_of(id_cols), all_of(ind2[!duplicated(sig)]))
  
  final_inds <- setdiff(names(kept), id_cols)
  message("Final indicators: ", length(final_inds))
  
  latest <- kept %>%
    arrange(iso3c, desc(year)) %>%
    group_by(iso3c, iso2c, country) %>%
    summarize(
      across(all_of(final_inds),
             ~ { v <- .x[!is.na(.x)]; if (length(v)) v[1] else NA_real_ }),
      .groups = "drop"
    ) %>%
    arrange(country)
  
  panel_csv  <- file.path(out_dir, paste0(theme, "_lac_panel_2000_2025.csv"))
  panel_xlsx <- file.path(out_dir, paste0(theme, "_lac_panel_2000_2025.xlsx"))
  latest_csv <- file.path(out_dir, paste0(theme, "_lac_latest_country_wide.csv"))
  latest_xlsx<- file.path(out_dir, paste0(theme, "_lac_latest_country_wide.xlsx"))
  
  write_csv(kept, panel_csv)
  write_xlsx(list(panel_2000_2025 = kept), panel_xlsx)
  write_csv(latest, latest_csv)
  write_xlsx(list(latest_country_wide = latest), latest_xlsx)
  
  message("Saved: ", basename(panel_csv))
  invisible(TRUE)
}

for (t in themes) harmonize_one(t)

cat("\nAll themes complete.\n")
