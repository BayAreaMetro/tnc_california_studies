# 02_join_weights.R
# 19 Feb 2021
# RSG, Inc.

# Joins weights from 01_make_weights.R to the appropriate tables

# Script parameters and options -----------------------------------------------

options(scipen = 999)

# define which study to run
# region = 'bayarea'
# region = 'sandag'
# region = 'scag'

message('region is ', region)


# Libraries and functions ------------------------------------------------------

library('data.table')
library('magrittr')
library('stringr')

# Set paths  ------------------------------------------------------------

if ( !exists('data_dir') ){
  data_dir = '.' # File path where codebook is/where data should be saved
}

if ( !exists('tsv_dir') ){
  tsv_dir = '.' # File path where TSV with data delivered Feb 2020 is
}

if ( !exists('work_dir') ){
  work_dir = '.'  # Directory for temporary data
}


# Load data --------------------------------------------------------------------

# Load weights

wts_path = file.path(work_dir, 'popsim_wt_person.csv')
wts = fread(wts_path)

# Keep only relevant regions
Region = region
wts = wts[region == Region]
wts[, region := NULL]
  
wts_day_path = file.path(work_dir, 'popsim_wt_day.csv')
wts_day = fread(wts_day_path)
wts_day = wts_day[region == Region]
wts_day[, region := NULL]

# Calculate travel_date_dow from dow
# In the codebook, Monday = 1, Sunday = 7
wts_day[, .N, keyby=dow]
wday_names = c('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun')

wts_day$travel_date_dow = sapply(wts_day[, dow], function(x) str_which(wday_names, x))
wts_day[, .N, keyby=.(travel_date_dow, dow)]

# Load survey data

hh = fread(file.path(tsv_dir, 'ex_hh.tsv'))
person = fread(file.path(tsv_dir, 'ex_person.tsv'))
day = fread(file.path(tsv_dir, 'ex_day.tsv'))

# Trip file has been modified so read from a different location
trip = readRDS(file.path(work_dir, paste0('trip_transit_corrected_', region, '.rds')))

trip_linked = readRDS(file.path(work_dir, paste0('trips_linked_', region, '.rds')))  


# Join weights ================================================================


# Person -----------------------------------------------------------------------

# Check match between person_ids
length(setdiff(wts[, person_id], person[, person_id]))
length(setdiff(person[, person_id], wts[, person_id]))

# Get rows for all person_id and fill NA with zeros
wts_person_all = merge(person[, .(person_id)], wts, by='person_id', all.x=TRUE)
fill_cols = c(
  str_subset(names(wts), '^wt_'),
  str_subset(names(wts), '_status'),
  str_subset(names(wts), '_complete'),
  'initial_wt')

setnafill(wts_person_all, fill=0, cols = fill_cols )

# check for person_ids in weights but not in personid
stopifnot(length(setdiff(wts_person_all[, person_id], person[, person_id])) == 0)


# Drop any weight columns from person from previous attempts

names_person_drop = intersect(names(person), names(wts_person_all))
(names_person_drop = names_person_drop[names_person_drop != 'person_id'])

person = person[, (names_person_drop) := NULL]

# join the two datasets
person_wt = merge(person, wts_person_all, by='person_id', all.x=TRUE)


# Check for missing values - should be none
stopifnot(
  sum(is.na(person_wt[, wt_sphone_wkday])) == 0,
  sum(is.na(person_wt[, wt_alladult_wkday])) == 0,
  sum(is.na(person_wt[, wt_sphone_7day])) == 0,
  sum(is.na(person_wt[, wt_alladult_7day])) == 0,
  person_wt[, .N] == person[, .N]
)


# HH --------------------------------------------------------------------------

# The household weight is the average weight across all adults in the HH,
# including any 0-weights for non-participant adults. 

# Calculate hh weights

wts_hh = wts[, .(person_id, subregion,
                 wt_sphone_wkday, wt_alladult_wkday,
                 wt_sphone_7day, wt_alladult_7day)]

# Get a row for all hh_id (plus get the hh_id column)
# Merge subregion separately so it doesn't get averaged

hh_subregion = merge(
  person[, .(hh_id, person_id)],
  wts_hh[, .(person_id, subregion)],
  by = 'person_id')

stopifnot(wts_hh[, .N] == hh_subregion[, .N])

hh_subregion[, person_id := NULL]
hh_subregion = unique(hh_subregion)
wts_hh[, subregion := NULL]

wts_hh_all = merge(
  person[, .(hh_id, person_id)],
  wts_hh,
  by = 'person_id', all.x=TRUE
  )

stopifnot(
  length(
    setdiff(wts_hh_all[, hh_id], hh[, hh_id]) ) == 0
)

wts_hh_all[, person_id := NULL]
setnafill(wts_hh_all, type='const', fill=0)

wts_hh_mean = wts_hh_all[, lapply(.SD, mean), by=hh_id]

stopifnot(length(setdiff(wts_hh_mean[, hh_id], hh[, hh_id])) == 0)

wts_hh_mean1 = merge(
  wts_hh_mean,
  hh_subregion,
  by = 'hh_id',
  all.x=TRUE)

stopifnot(nrow(wts_hh_mean1) == nrow(wts_hh_mean))

# Drop any weight columns from hh from previous attempts

names_hh_drop = intersect(names(hh), names(wts_hh_all))
(names_hh_drop = names_hh_drop[names_hh_drop != 'hh_id'])

hh[, (names_hh_drop) := NULL]

hh_wt = merge(hh, wts_hh_mean1, by = 'hh_id', all.x=TRUE)

stopifnot(nrow(hh) == nrow(hh_wt))

# Check for missing values - should be none
stopifnot(
  sum(is.na(hh_wt[, wt_sphone_wkday])) == 0,
  sum(is.na(hh_wt[, wt_sphone_7day])) == 0,
  sum(is.na(hh_wt[, wt_alladult_wkday])) == 0,
  sum(is.na(hh_wt[, wt_alladult_7day])) == 0
)


# Day --------------------------------------------------------------------------

# Drop any weight columns from day 

names_day_drop = intersect(names(day), names(wts_day))
(names_day_drop = names_day_drop[!names_day_drop %in% c('person_id', 'travel_date_dow')])

day[, (names_day_drop) := NULL]


# Get a row for every day, and fill NAs with zero
wts_dt = merge(
  day[, .(person_id, travel_date_dow)],
  wts_day,
  by = c('person_id', 'travel_date_dow'), all.x=TRUE)

wt_cols = str_subset(names(wts_dt), '^daywt')
setnafill(wts_dt, fill=0, cols=c(wt_cols))


day_wt = merge(
  day,
  wts_dt,
  by = c('person_id', 'travel_date_dow'))

stopifnot(nrow(day) == nrow(day_wt),
          sum(is.na(day_wt[, daywt_sphone_7day])) == 0,
          sum(is.na(day_wt[, daywt_sphone_wkday])) == 0,
          sum(is.na(day_wt[, daywt_alladult_7day])) == 0,
          sum(is.na(day_wt[, daywt_alladult_wkday])) == 0,
          sum(is.na(day_wt[, daywt_sphone_dow])) == 0,
          sum(is.na(day_wt[, daywt_alladult_dow])) == 0)


# Wkday weights are zeroed for non-complete weekdays

day_wt[travel_date_dow > 4, daywt_alladult_wkday := 0]
day_wt[travel_date_dow > 4, daywt_sphone_wkday := 0]
day_wt[day_status < 9, daywt_alladult_wkday := 0]
day_wt[day_status < 9, daywt_sphone_wkday := 0]


# Sum of weights in day table equal the sum of person table weights
stopifnot(
  all.equal(sum(day_wt[, daywt_alladult_wkday]),
    sum(person_wt[, wt_alladult_wkday])),
  all.equal(sum(day_wt[, daywt_sphone_wkday]),
    sum(person_wt[, wt_sphone_wkday])),
  all.equal(sum(day_wt[, daywt_alladult_7day]),
    sum(person_wt[, wt_alladult_7day])),
  all.equal(sum(day_wt[, daywt_sphone_7day]),
    sum(person_wt[, wt_sphone_7day])),
  
  # Check for non-zero weights on incomplete days
  day_wt[day_status < 9 & (daywt_sphone_wkday > 0), .N] == 0,
  day_wt[day_status < 9 & (daywt_alladult_wkday > 0), .N] == 0
)


# Trip -------------------------------------------------------------------------

# Get a subset of the day_wt to join with trip
keep_cols = c('person_id','day_num', wt_cols)
day_wt_merge = day_wt[, keep_cols, with=FALSE]

# Drop any weight columns from trip

names_trip_drop = intersect(names(trip), wt_cols)

trip[, (names_trip_drop) := NULL]


# Find trips that don't have matches in the weight file
no_wts = trip[
  !day_wt_merge,
  on = c('person_id', 'day_num')][
    , .(person_id, day_num)]

stopifnot(no_wts[day_num > 0, .N] == 0)

trip_wt = merge(
  trip,
  day_wt_merge,
  by = c('person_id', 'day_num'),
  all.x	= TRUE # to ensure we don't lose day_num == 0 trips.
)

setnafill(trip_wt,
          type='const',
          cols=str_subset(names(trip_wt), 'daywt_'),
          fill=0)

stopifnot(
  nrow(trip) == nrow(trip_wt),
  sum(is.na(trip_wt[, daywt_sphone_wkday])) == 0,
  sum(is.na(trip_wt[, daywt_alladult_wkday])) == 0,
  sum(is.na(trip_wt[, daywt_sphone_7day])) == 0,
  sum(is.na(trip_wt[, daywt_alladult_7day])) == 0
)

# Wkday weights are zeroed for non-complete weekdays

# # Get an index of trips that are valid to have non-zero weights
# wkday_idx = trip_wt[, 
#   travel_date_dow <= 4 | (travel_date_dow == 5 & hour(depart_time) < 3)]
# 
# # Small number of exceptions result from synthetic trip access and egress
# stopifnot(
#   trip_wt[!wkday_idx & !transit_quality_flag %in% c('SA', 'SE') & daywt_sphone_wkday > 0, .N] == 0,
#   trip_wt[!wkday_idx &  !transit_quality_flag %in% c('SA', 'SE') & daywt_alladult_wkday > 0, .N] == 0,
#   trip_wt[day_status < 9 & daywt_alladult_wkday > 0, .N] == 0,
#   trip_wt[day_status < 9 & daywt_sphone_wkday > 0, .N] == 0
# )

# Linked trip ----------------------------------------------------------------

# Drop weights columns
# trip_linked[, (names_trip_drop) := NULL]

trip_linked_wt = merge(
  trip_linked,
  day_wt_merge,
  by = c('person_id', 'day_num'),
  all.x = TRUE)

setnafill(trip_linked_wt, cols=wt_cols, fill=0)

stopifnot(
  nrow(trip_linked) == nrow(trip_linked_wt),
  sum(is.na(trip_linked_wt[, daywt_sphone_wkday])) == 0,
  sum(is.na(trip_linked_wt[, daywt_alladult_wkday])) == 0,
  sum(is.na(trip_linked_wt[, daywt_sphone_7day])) == 0,
  sum(is.na(trip_linked_wt[, daywt_alladult_7day])) == 0
)

# Wkday weights are zeroed for non-complete weekdays
# 
# wkday_idx = trip_linked_wt[, 
#   travel_date_dow <= 4 | (travel_date_dow == 5 & hour(depart_time) < 3)]
# 
# stopifnot(
#   trip_linked_wt[!wkday_idx & daywt_sphone_wkday > 0, .N] == 0,
#   trip_linked_wt[!wkday_idx & daywt_alladult_wkday > 0, .N] == 0,
#   trip_linked_wt[day_status < 9 & daywt_alladult_wkday > 0, .N] == 0,
#   trip_linked_wt[day_status < 9 & daywt_sphone_wkday > 0, .N] == 0
# )


# Write data ===================================================================
# as RDS to work_dir

saveRDS(hh_wt, file.path(work_dir, str_glue('hh_with_weights_{region}.rds')))
saveRDS(person_wt, file.path(work_dir, str_glue('person_with_weights_{region}.rds')))
saveRDS(day_wt, file.path(work_dir, str_glue('day_with_weights_{region}.rds')))
saveRDS(trip_wt, file.path(work_dir, str_glue('trip_with_weights_{region}.rds')))
saveRDS(trip_linked_wt, file.path(work_dir, str_glue('trip_linked_with_weights_{region}.rds')))


