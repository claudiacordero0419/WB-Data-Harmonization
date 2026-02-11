
#Download packages
suppressPackageStartupMessages({
	library(readr)
	library(dplyr)
	library(tidyr)
	library(stringr)
	library(janitor)
	library(writexl)
	library(wbstats)
})

#Settings
start_year <- 2001
end_year <- 2025
n_countries <- 50

in_path <- "output/social_infrastructure_wide_2000_2025.csv"
out_dir <- "output"
dir.create(out_dir, showWarnings=FALSE)

stopifnot(file.exists(in_path))

#Load external debt wide-format data
panel_raw <- readr::read_csv(in_path, show_col_types=FALSE)

#Convert World Bank "date" column
panel_raw <- panel_raw %>%
	mutate (
		iso3c = toupper(str_trim(as.character(iso3c))),
		year=as.numeric(date)
	)
	
required_cols <- c("iso3c", "country", "date")
missing_cols <- setdiff(required_cols, names(panel_raw))
if(length(missing_cols) > 0) {
	stop(paste("Missing required columns:", paste(missing_cols, collapse=", ")))
}

#Normalize the time column to either expect a date or year in numeric form
if("year" %in% names(panel_raw)) {
	panel_raw <- panel_raw %>% mutate(year=as.numeric(year))
}   else if ("date" %in% names(panel_raw)) {
	panel_raw <- panel_raw %>% mutate(date=as.numeric(date))
}   else {
	stop("Could not find a 'date' or 'year' column in eg_panel_wide.csv")
}


#Ensure ID-columns exist
id_candidates <- c("iso3c", "iso2c", "country", "year")
missing_ids <- setdiff(c("iso3c", "country", "year"), names (panel_raw))
if(length(missing_ids) > 0) {
	stop(paste("Missing required columns: ", paste(missing_ids, collapse =", ")))
}

#Ensure only data from the years 2001 - 2025 is shown
panel_raw <- panel_raw %>%
	filter(year >= start_year & year <= end_year)

#Filter through countries with iso3c codes that exist and select the approrpaite columns
#Build LATAM + Caribbean country list using WB metadata

wb_meta <- wbstats::wb_countries()

region_col <- dplyr::case_when(
	"region.value" %in% names(wb_meta) ~ "region.value",
	"region" %in% names(wb_meta) ~"region",
	TRUE ~ NA_character_
)
	
if (is.na(region_col)) {
	stop("Could not find a region column in wb_countries(). Expected 'region.value' or 'region'.")
}

wb_meta2 <- wb_meta %>% 
	mutate(
		iso3c = toupper(str_trim(as.character(iso3c))),
		region_txt = as.character(.data[[region_col]])
	)
	
latam_iso3 <- wb_meta2 %>%
	filter(region_txt == "Latin America & Caribbean" ) %>%
	pull(iso3c) %>%
	unique()
	
panel_latam <- panel_raw %>%
	mutate(iso3c = toupper(str_trim(as.character(iso3c)))) %>%
	filter(iso3c %in% latam_iso3)
	
#Pick 50 LATAM countries
iso3_list <- sort(unique(panel_latam$iso3c))
top_latam <- head(iso3_list, n_countries)

panel_latam50 <- panel_latam %>%
	filter(iso3c %in% top_latam)

#Identify indicator columns
id_cols <- intersect(c("iso3c", "iso2c", "country", "year"), names(panel_latam50))
ind_cols <- setdiff(names(panel_latam50), c(id_cols, "date", "region", "adminregion", "income_level"))

#Make sure indicator names are unique (drop duplicate names by keeping the first)
dup_name_flags <- duplicated(ind_cols)
if(any(dup_name_flags)) {
	message("Found duplicate indicator columns NAMES after cleaning. Keeping the first instance")
	ind_cols_unique <-ind_cols[!dup_name_flags]
	panel_latam50 <- panel_latam50 %>% select(all_of(id_cols), all_of(ind_cols_unique))
    ind_cols <- ind_cols_unique
}




#----Ensure complete country-year grid before applying the missingness filter----
# Panel IDs (includes year)
id_cols_panel <- intersect(c("iso3c", "iso2c", "country", "year"), names(panel_latam50))

# Country IDs only (NO year)
id_cols_country <- intersect(c("iso3c", "iso2c", "country"), names(panel_latam50))

grid <- expand.grid(
  iso3c = sort(unique(panel_latam50$iso3c)),
  year  = seq(start_year, end_year),
  stringsAsFactors = FALSE
) %>%
  as_tibble() %>%
  left_join(
    panel_latam50 %>% select(all_of(id_cols_country)) %>% distinct(),
    by = "iso3c"
  ) %>%
  select(iso3c, iso2c, country, year)

panel_complete <- grid %>%
  left_join(
    panel_latam50 %>% select(all_of(id_cols_panel), all_of(ind_cols)),
    by = c("iso3c", "iso2c", "country", "year")
  )
		
#Drop indicators with >50% missingness

#Finds the mean value of all missing data for each indicator column
miss_rate <- sapply(panel_complete[ind_cols], function(x) mean(is.na(x)))

#Keep indicators with a missingness rate of less than 0.5
keep_inds <- names(miss_rate)[miss_rate <= 0.50]

id_cols <- intersect(c("iso3c","iso2c","country","year"), names(panel_complete))
panel_kept <- panel_complete %>% select(all_of(id_cols), all_of(keep_inds))
	
message("Indicators before missingness filter: ", length(ind_cols))
message("Indicators kept(<=50% missing): ", length(keep_inds))

#----Remove redundant indicators----

ind2 <- setdiff(names(panel_kept), id_cols)

col_sig <- sapply(panel_kept[ind2], function(x) {
  paste0(ifelse(is.na(x), "NA", format(x, scientific = FALSE, digits = 15)), collapse = "|")
})

drop_identical <- duplicated(col_sig)
if(any(drop_identical)) {
	message("Dropping ", sum(drop_identical), "redundant (identical) indicators")
}

panel_final <- panel_kept %>%
	select(all_of(id_cols), all_of(ind2[!drop_identical]))
	
id_base <- c("iso3c", "iso2c", "country", "year")
ind_cols_final <- setdiff(names(panel_final), id_base)

country_missing <- panel_final %>%
  rowwise() %>%
  mutate(non_na = sum(!is.na(c_across(all_of(ind_cols_final))))) %>%
  ungroup() %>%
  group_by(iso3c, country) %>%
  summarize(avg_non_na = mean(non_na), .groups = "drop") %>%
  arrange(avg_non_na)

print(head(country_missing, 10))
	
final_indicators <- setdiff(names(panel_final), id_cols)
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
panel_csv <- file.path(out_dir, "social_infrastructure_latam50_panel_25y.csv")
panel_xlsx <- file.path(out_dir, "social_infrastructure_latam50_panel_25y.xlsx")

latest_csv <- file.path(out_dir, "social_infrastructure_latest_country_wide.csv")
latest_xlsx <- file.path(out_dir, "social_infrastructure_latest_country_wide.xlsx")

write_csv(panel_final, panel_csv)
write_xlsx(list(panel_25y = panel_final), panel_xlsx)

write_csv(latest_country, latest_csv)
write_xlsx(list(latest_country_wide=latest_country), latest_xlsx)

cat("\nDONE \nSaved:\n",
    " - ", normalizePath(panel_csv,  winslash="/"), "\n",
    " - ", normalizePath(panel_xlsx, winslash="/"), "\n",
    " - ", normalizePath(latest_csv,  winslash="/"), "\n",
    " - ", normalizePath(latest_xlsx, winslash="/"), "\n", sep = "")