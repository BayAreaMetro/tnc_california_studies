# 01_update_num_travelers.R
# Script to incorporate imputed number of travelers 


# setup =======================================================================

# Load libraries

library(stringr)
library(data.table)
library(lubridate)


# Set paths ------------------------------------------------------------------ 
# (Assumes working directory is location of this file)


# # Sharepoint where data are
# sp_dir = str_glue(
#   'C:/Users/{Sys.info()["user"]}/Resource Systems Group, Inc/',
#   'Transportation MR - Documents/20261_TNC_DatasetImprovements')
# 
# dbnames = c('tnc_bayarea', 'tnc_sandag', 'tnc_scag')
# 
# regions = c('BayArea', 'SANDAG', 'SCAG')
# names(regions) = dbnames
# 
# data_dir = file.path(sp_dir, 'Data')
# tsv_dirs = file.path(data_dir, paste0(regions, '_dataset_2020-02-27'))
# names(tsv_dirs) = dbnames
# 
# tsv_new_dirs = file.path(data_dir, 'Final_data_2021', regions)
# names(tsv_new_dirs) = dbnames
# 
# # Create a working directory to hold temporary datasets
# work_dir = file.path(data_dir, 'Working_data')


# Work =======================================================================

for ( dbname in dbnames ){
  
  region = str_replace(dbname, 'tnc_', '')
  message('Merging imputed num_travelers for ', region)
  
  tsv_dir = tsv_dirs[dbname]
  
  # Load original trip file to crosswalk trip_num with trip_id
  og_file = file.path(tsv_dir, 'ex_trip.tsv')
  trip_og = fread(og_file)
  dt_trip_num = trip_og[, .(hh_id, person_num, trip_num, trip_id)]
  
  # Load trip file (output from mode imputation script)
  trip = readRDS(file.path(work_dir, str_glue('trip_new_mode_tnc_{region}.rds')))
  
  # Load num travelers imputed
  impute_file = file.path('model_files', str_glue('{region}_numtrav_imputed.dat'))
  nt = fread(impute_file)
  
  # Merge to get trip_id 
  nt1 = merge(
    dt_trip_num,
    nt,
    by = c('hh_id','person_num', 'trip_num'),
    all=TRUE)
  
  stopifnot(dt_trip_num[, .N] == nt1[, .N])
  
  # Merge with updated trip file
  
  trip_new = merge(
    trip,
    nt1[, .(trip_id, num_trav_nt = num_travelers, num_travelers_imputed, nt_impute_type)],
    by = c('trip_id'),
    all = TRUE)
  
  stopifnot(
    trip_new[, .N] == trip[, .N],
    trip_new[num_travelers != num_trav_nt, .N] == 0)
  
  trip_new[, num_trav_nt := NULL]
  
  # Fill in num_travelers_imputed and nt_impute_type where missing
  
  trip_new[is.na(num_travelers_imputed), nt_impute_type := -1]
  trip_new[is.na(num_travelers_imputed), num_travelers_imputed := num_travelers]

  stopifnot(  
    trip_new[is.na(num_travelers_imputed), .N] == 0,
    trip_new[is.na(nt_impute_type), .N] == 0
  )

  # Save result
  outfile = file.path(work_dir, str_glue('trip_new_numtravelers_{region}.rds'))
  saveRDS(trip_new, outfile)
  message('Wrote output to ', basename(outfile))
}
