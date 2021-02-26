# update_export_tables.R

# 17 February 2021.
# matt.landis@rsginc.com

# This script reads in export tables as delivered in Feb 2020 and updates
# with all of the actions from Task Order 8

# What should we update?
# A safety check to avoid inadvertent updates

update_mode = FALSE
update_num_travelers = FALSE
update_transit = TRUE
update_weighting = TRUE
recalc_weights = FALSE


# Setup ======================================================================

library(rprojroot)
library(stringr)
library(data.table)
library(magrittr)

# Git root
root_dir = file.path(find_root(is_git_root), 'TaskOrder8')

# Sharepoint where data are

user = Sys.info()['user']

if(user == 'leah.flake'){
  sp_dir = str_glue(
    'C:/Users/{Sys.info()["user"]}/Resource Systems Group, Inc/',
    'Transportation MR - 20261_TNC_DatasetImprovements')
} else {
  sp_dir = str_glue(
    'C:/Users/{Sys.info()["user"]}/Resource Systems Group, Inc/',
    'Transportation MR - Documents/20261_TNC_DatasetImprovements')
}



dbnames = c('tnc_bayarea', 'tnc_sandag', 'tnc_scag')
regions = c('BayArea', 'SANDAG', 'SCAG')

# dbnames = c('tnc_bayarea', 'tnc_sandag')
# regions = c('BayArea', 'SANDAG')

names(regions) = dbnames

data_dir = file.path(sp_dir, 'Data')
tsv_dirs = file.path(data_dir, paste0(regions, '_dataset_2020-02-27'))
names(tsv_dirs) = dbnames

tsv_new_dirs = file.path(data_dir, 'Final_data_2021', regions)
names(tsv_new_dirs) = dbnames
lapply(tsv_new_dirs, function(x) dir.create(x, showWarnings=FALSE, recursive=TRUE))

# Create a working directory to hold temporary datasets
work_dir = file.path(data_dir, 'Working_data')
dir.create(work_dir, showWarnings=FALSE)


# 8.4 mode imputation ========================================================

if ( update_mode ){
  dir_84 = file.path(root_dir, '8.4_mode_imputation')
  
  # Updated trip tables created with the following 
  # (don't expect these to run)
  
  # source(file.path(dir84, '01_fit_mode_apollo.R'))
  # for (dbname in c('tnc_bayarea', 'tnc_sandag', 'tnc_scag')){
  #   source(file.path(dir84, '03_impute_mode_type.R'), local=new.env())
  # }
  
  # Data saved to file.path(dir_84, dbname, 'v2.1')
  
  for ( dbname in dbnames ){
    trip_mode = fread(
      file.path(dir_84, 'model_data', str_glue('trip_{dbname}_v2.1.csv'))
    )
    
    mode_cols = str_subset(names(trip_mode), 'mode_(type|probability)(?!.*del$)')
    trip_mode = trip_mode[, c('trip_id', mode_cols), with=FALSE]
    
    message(str_glue('Updating {dbname}'))
    # Read the original file
    tsv_dir = tsv_dirs[dbname]
    
    trip = fread(file.path(tsv_dir, 'ex_trip_with_purpose_other.tsv'))
    
    # drop the old mode columns
    trip[, (mode_cols) := NULL]
    trip_new = merge(
      trip,
      trip_mode,
      by = 'trip_id')
    
    stopifnot(
      trip[, .N] == trip_mode[, .N],
      trip[, .N] == trip_new[, .N]
    )
    
    # Save the updated trip file
    outfile = file.path(work_dir, str_glue('trip_new_mode_{dbname}.rds'))
    saveRDS(trip_new, outfile)
  }
  
  message('Done updating mode columns')
}


# 8.3 Num travelers =========================================================

if ( update_num_travelers ){
  
  dir_83 = file.path(root_dir, '8.3_num_travelers')
  setwd(dir_83)

  source('01_update_num_travelers.R', local=new.env())
}


# 8.2 transit improvement ====================================================

if ( update_transit ){
  
  dir_82 = file.path(root_dir, '8.2_transit_improvement')
  setwd(dir_82)
  
  for ( region in str_to_lower(regions) ){
    
    tsv_dir = tsv_dirs[paste0('tnc_', region)]
    
    message('Running 01_transit_trip_relinking.R')
    source('01_transit_trip_relinking.R', local=new.env())
    
    message('Running 02_transit_insert_access_egress.R')
    source('02_transit_insert_access_egress.R', local=new.env())
    
    message('Running 03_linked_trip_table.R')
    source('03_linked_trip_table.R', local=new.env())
    
  }

}


# 8.1 PopSim weighting =======================================================

if ( update_weighting | recalc_weights ){
  dir_81 = file.path(root_dir, '8.1_PopSim_weighting')
  setwd(dir_81)
  
  if ( recalc_weights ){
    
    unlink(file.path(work_dir, 'PopSim_results'), recursive=TRUE, force=TRUE)
    
    source('01_make_weights.R', local=new.env())
  }
  
  # Update weights in hh, person, day, and trip files

  message('Joining weights to data files')
  for ( region in str_to_lower(regions) ){
    
    tsv_dir = tsv_dirs[paste0('tnc_', region)]
    source('02_join_weights.R', local=new.env())
    
  }
}


# Create final files =====================================================
# Write / copy all tables to export location as TSV

for (region in str_to_lower(regions)){
  
  tsv_dir = tsv_dirs[paste0('tnc_', region)]
  tsv_new_dir = tsv_new_dirs[paste0('tnc_', region)]
  
  message('Writing final output files to ', tsv_new_dir)
  
  # HH
  hh = readRDS(file.path(work_dir, str_glue('hh_with_weights_{region}.rds')))
  fwrite(hh, file.path(tsv_new_dir, 'hh.tsv'), sep = '\t')
  
  # Person
  person = readRDS(file.path(work_dir, str_glue('person_with_weights_{region}.rds')))
  
  person[gender_imputed == 0, gender_imputed := 995L]
  person[income_imputed == 0, income_imputed := 995L]
  
  fwrite(person, file.path(tsv_new_dir, 'person.tsv'), sep='\t')
  
  # Day
  day = readRDS(file.path(work_dir, str_glue('day_with_weights_{region}.rds')))
  drop_cols = c('dow', 'nwkdaywts_complete')
  day[, (drop_cols) := NULL]
  
  fwrite(day, file.path(tsv_new_dir, 'day.tsv'), sep='\t')
  
  # Trip
  trip = readRDS(file.path(work_dir, str_glue('trip_with_weights_{region}.rds')))
  setnames(trip, 'rep_access_recode', 'access_mode_type_reported')
  setnames(trip, 'rep_egress_recode', 'egress_mode_type_reported')
  
  drop_cols = c(
    'rm_trip_id', 'fmr_purpose_imputed',
    'day_complete', 'is_transit_leg')
  
  trip[, (drop_cols) := NULL]
  
  fwrite(trip, file.path(tsv_new_dir, 'trip_with_purpose_other.tsv'), sep='\t')
  
  # drop purpose other columns
  drop_cols = c('o_purpose_other', 'd_purpose_other')
  trip[, (drop_cols) := NULL]
  
  fwrite(trip, file.path(tsv_new_dir, 'trip.tsv'))
  
  # Linked trip
  ltrip = readRDS(file.path(work_dir, str_glue('trip_linked_with_weights_{region}.rds')))
  setnames(ltrip, 'rep_access_recode', 'access_mode_type_reported', skip_absent=TRUE)
  setnames(ltrip, 'rep_egress_recode', 'egress_mode_type_reported', skip_absent=TRUE)
  
  drop_cols = c(
    'leg_1_trip_id', 'day_complete', 'rep_access_mode', 'rep_egress_mode')
  
  ltrip[, (drop_cols) := NULL]
  
  fwrite(ltrip, file.path(tsv_new_dir, 'trip_linked.tsv'), sep='\t')
  
  # Location
  file.copy(
    file.path(tsv_dir, 'ex_location.tsv'),
    file.path(tsv_new_dir, 'location.tsv'))
  
  # Vehicle
  file.copy(
    file.path(tsv_dir, 'ex_vehicle.tsv'),
    file.path(tsv_new_dir, 'vehicle.tsv'))
  
  # Create zip_file -----------------------------------------------------------
  files = Sys.glob(file.path(tsv_new_dir, '*.tsv'))
  
  pw = 'thesearethefilesfromRSGinc'
  zip(zip_path, files=files,
    flags=str_glue('-j --password {pw}'))
  
}



# End
