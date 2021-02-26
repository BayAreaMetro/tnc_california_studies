# Access/egress trip creation
# 2 Feb 2021, RSG, Inc.

# Code assumes working directory is location of this file

# Set region to work on
# region = 'bayarea'
# region = 'sandag'
# region = 'scag'

message('region is ', region)

# Load libraries -------------------------------------------------------------

necessary_packages = c("data.table", "stringr", "magrittr", "lubridate")

new_packages = setdiff(necessary_packages, installed.packages()[,"Package"])

if(length(new_packages) > 0) {
  install.packages(new_packages)
}

invisible(lapply(necessary_packages, library, character.only = TRUE))


source('_functions.R')


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

codebook_path = file.path(data_dir, 'Consolidated_SB1_TNC_Study_Codebook_March2021.xlsx')


# Load settings -------------------------------------------------------------------

# Load codebook

codebook = read_codebook(codebook_path)

codebook_label_col = switch(region,
  bayarea = 'label_mtc',
  sandag = 'label_sandag',
  scag = 'label_scag')

setnames(codebook, codebook_label_col, 'label_long')
codebook[, label_short := label_long]
codebook[, label_short := str_replace(label_short, 'Missing: ', '')]
codebook[, label_short := str_replace(label_short, ' [(].*[)]$', '')]
codebook[, label_short := str_trunc(label_short, width=20, side='right')]
codebook[, value_label := paste(value, label_short)]
codebook = codebook[, .(variable, value, label_long, value_label, label_short)]

variable_labels = read_codebook(codebook_path, varvals=FALSE)
variable_labels = variable_labels[, .(variable, data_type)]

# get values
mode_type_walk = codebook[variable == 'mode_type' & label_long %like% 'Walk', value]

mode_type_transit = codebook[
  variable == 'mode_type' &
    label_long %like% 'Transit', value]

purpose_change_mode = codebook[
  variable == 'd_purpose' &
    label_long %like% 'Change/transfer', value]

purpose_category_change_mode = codebook[
  variable == 'd_purpose_category' &
    label_long == 'Change mode', value]

missing_values= codebook[
  variable == '_All categorical variables_' &
    label_long %like% "Missing", value]

missing_non_response_value = codebook[
  variable == '_All categorical variables_' &
    label_long == "Missing: Non-response", value]

missing_skip_value = codebook[
  variable == '_All categorical variables_' &
    label_long == "Missing: Skip logic", value]

mode_code_bus = c(28, 46, 55)
mode_code_rail = c(30, 39, 42, 68)
mode_code_ferry = 32
mode_code_intercity_rail = 41


mode_priority = fread(file.path(data_dir, 'mode_importance_ranking.csv'))


# Load data ------------------------------------------------------------------

message('Loading data')
# Get colnames as delivered (for write out at the end)
trip_delivered_cols = names(fread(
  file.path(tsv_dir, 'ex_trip_with_purpose_other.tsv'),
  nrows=1))

# Load newly re-linked trip data
trip_rl = readRDS(file.path(work_dir, paste0('trip_transit_relinked_', region, '.rds')))
trip_colnames = names(trip_rl) %>% copy()


# Create records to insert ---------------------------------------------------

message('Creating records to insert')

trip_rl[, is_first := 1 * (leg_num == 1)]
trip_rl[, is_last := 1 * (leg_num == n_legs)]

trip_rl[
  , max_trip_id := max((as.numeric(trip_id)) - ((person_id) * 1000)),
  by = .(person_id)] # for setting trip_id later

trip_rl[, linked_trip_num := NULL]
trip_rl[, c('travel_date', 'travel_date_dow') := NULL] # remove as these are not meaningful in the trip tables

imputed_access_trips = trip_rl[is_transit == 1 & has_linked_access == 0 & is_first == 1]
imputed_egress_trips = trip_rl[is_transit == 1 & has_linked_egress == 0 & is_last == 1]

# Only transit trips should have rep_access/egress_recode
imputed_access_trips[, .N, .(rep_access_recode, mode_type)
  ][order(rep_access_recode, mode_type)]
imputed_egress_trips[, .N, .(rep_egress_recode, mode_type)
  ][order(rep_egress_recode, mode_type)]


stopifnot(
  all(imputed_access_trips[, .N, .(linked_trip_id)][, N==1]),
  all(imputed_egress_trips[, .N, .(linked_trip_id)][, N==1])
)


# Set values for access / egress trips --------------------------------------

message('Setting values for access/egress trips')

imputed_access_trips[, `:=` (
  leg_num = 0, # this will get re-derived (will end up being 1), keeping sequential for now
  trip_num = 0, # placeholder
  mode_type = fifelse(
    !is.na(rep_access_recode) &
      rep_access_recode != missing_non_response_value &
      rep_access_recode != mode_type_transit,
    rep_access_recode,
    mode_type_walk),
  mode_type_imputed = fifelse(
    !is.na(rep_access_recode) &
      rep_access_recode != missing_non_response_value &
      rep_access_recode != mode_type_transit,
    rep_access_recode,
    mode_type_walk),
  d_purpose = purpose_change_mode,
  d_purpose_other = '',
  d_purpose_category = purpose_category_change_mode,
  d_purpose_imputed = purpose_change_mode,
  d_purpose_category_imputed = purpose_category_change_mode,
  is_primary_leg = 0,
  is_linked_access = 1,
  is_transit_leg = 0,
  is_first = 1,
  arrive_time = depart_time,
  arrive_hour = depart_hour,
  arrive_minute = depart_minute,
  dwell_time_min = 0
  )]

imputed_egress_trips[, `:=` (
  leg_num = leg_num + 1, # this will get re-derived in case of imputed access
  trip_num = 999, # placeholder
  mode_type = fifelse(
    !is.na(rep_egress_recode) &
      rep_egress_recode != missing_non_response_value &
      rep_egress_recode != mode_type_transit,
    rep_egress_recode,
    mode_type_walk),
  mode_type_imputed = fifelse(
    !is.na(rep_egress_recode) &
      rep_egress_recode != missing_non_response_value &
      rep_egress_recode != mode_type_transit,
    rep_egress_recode,
    mode_type_walk),
  o_purpose = purpose_change_mode,
  o_purpose_other = '',
  o_purpose_category = purpose_category_change_mode,
  o_purpose_imputed = purpose_change_mode,
  o_purpose_category_imputed = purpose_category_change_mode,
  is_primary_leg = 0,
  is_linked_egress = 1,
  is_transit_leg = 0,
  is_last = 1,
  depart_time = arrive_time,
  depart_time_imputed = arrive_time,
  depart_hour = arrive_hour, 
  depart_minute = arrive_minute)]

# imputed_access_trips[, .N, .(rep_access_recode, mode_type)][order(rep_access_recode, mode_type)]
# imputed_egress_trips[, .N, .(rep_egress_recode, mode_type)][order(rep_egress_recode, mode_type)]


# Set missing values

hh_member_cols = trip_colnames[trip_colnames %like% 'hh_member_']
purpose_cols = trip_colnames[trip_colnames %like% 'purpose']

keep_cols = c('hh_id', 'person_id', 'person_num', 
  'linked_trip_id', 'is_transit',
  'day_num', 'day_num_12am', 
  'day_complete', hh_member_cols,
  'num_hh_travelers', 'num_non_hh_travelers',
  'num_travelers', 'num_travelers_imputed', 'day_status',
  'rep_access_mode', 'rep_egress_mode',
  'leg_num', 'mode_type', 'mode_type_imputed', 'is_primary_leg',
  'trip_num', 'max_trip_id', purpose_cols,
  'depart_time', 'arrive_time', 'depart_time_imputed', 'arrive_hour',
  'depart_hour', 'arrive_minute', 'depart_minute', 'dwell_time_min')


# Identify integer columns to set to 995
cols_to_995 = intersect(trip_colnames, codebook[, unique(variable)]) %>%
  setdiff(keep_cols)

cols_to_NA = setdiff(variable_labels[, variable], codebook[, unique(variable)]) %>%
  intersect(trip_colnames) %>%
  setdiff(keep_cols)

cols_to_NA = c(cols_to_NA, 'rm_trip_id')

imputed_access_trips[, (cols_to_995) := 995]
imputed_access_trips[, (cols_to_NA) := NA]

imputed_egress_trips[, (cols_to_995) := 995]
imputed_egress_trips[, (cols_to_NA) := NA]


# combine access/egress to set a trip ID -- don't re-set others, count up from highest trip_id for that person

imputed_ae_trips = rbindlist(
  list(access = imputed_access_trips, egress = imputed_egress_trips),
  idcol='imputed_transit_type')

imputed_ae_trips[, trip_id := as.character(trip_id)]
imputed_ae_trips[, trip_id := 
    paste0(person_id, str_pad(max_trip_id + seq_len(.N), 3, 'left', '0')), 
  by = .(person_id)]


# Combine with original trip dataset ------------------------------------------

message('Combining synthetic trips with existing trips')

trip_rl[, trip_id := as.character(trip_id)]
trip_with_ae = rbindlist(
  list(trip_rl, imputed_ae_trips),
  use.names = TRUE, fill = TRUE)

trip_with_ae = trip_with_ae[order(person_id, linked_trip_id, leg_num)]

# Add a linked_trip-wide flag 
trip_with_ae[is_transit == 1, c('has_synthetic_access', 'has_synthetic_egress') := 0]

trip_with_ae[
  linked_trip_id %in% imputed_access_trips[, linked_trip_id],
  has_synthetic_access := 1
  ]

trip_with_ae[
  linked_trip_id %in% imputed_egress_trips[, linked_trip_id],
  has_synthetic_egress := 1
]

# re-set trip_num, leg_num
trip_with_ae[, trip_num := seq_len(.N), by = .(person_id)]
trip_with_ae[, leg_num := seq_len(.N), by = .(linked_trip_id)]
trip_with_ae[, n_legs := .N, by = .(linked_trip_id)]

trip_with_ae[, is_first := 1 * (leg_num == 1)]
trip_with_ae[, is_last := 1 * (leg_num == n_legs)]

# Fix trips after added trips
# purposes

message('Trip cleanup')

trip_with_ae[has_synthetic_access == 1, `:=`(
  o_purpose = fifelse(
    leg_num == 2, get_prev(d_purpose), o_purpose),
  o_purpose_other = fifelse(
    leg_num == 2, get_prev(d_purpose_other), o_purpose_other),
  o_purpose_category = fifelse(
    leg_num == 2, get_prev(d_purpose_category), o_purpose_category),
  o_purpose_imputed = fifelse(
    leg_num == 2, get_prev(d_purpose_imputed), o_purpose_imputed), 
  o_purpose_category_imputed = fifelse(
    leg_num == 2, get_prev(d_purpose_category_imputed), o_purpose_category_imputed),
  o_purpose_impute_type = fifelse(
    leg_num == 2, get_prev(d_purpose_impute_type), o_purpose_impute_type)),
  by = .(linked_trip_id)]

trip_with_ae[has_synthetic_egress == 1, `:=`(
  d_purpose = fifelse(
    leg_num == (n_legs - 1), get_next(o_purpose), d_purpose),
  d_purpose_other = fifelse(
    leg_num == (n_legs - 1), get_next(o_purpose_other), d_purpose_other),
  d_purpose_category = fifelse(
    leg_num == (n_legs - 1), get_next(o_purpose_category), d_purpose_category),
  d_purpose_imputed = fifelse(
    leg_num == (n_legs - 1), get_next(o_purpose_imputed), d_purpose_imputed),
  d_purpose_category_imputed = fifelse(
    leg_num == (n_legs - 1), get_next(o_purpose_category_imputed), d_purpose_category_imputed),
  d_purpose_impute_type = fifelse(
    leg_num == (n_legs - 1), get_next(o_purpose_impute_type), d_purpose_impute_type),
  dwell_time_min = fifelse(
    leg_num == (n_legs - 1), 0, dwell_time_min)),
  by = .(linked_trip_id)]

trip_with_ae[is_transit == 1, `:=`(
  is_imputed_access = 0,
  is_imputed_egress = 0)]

trip_with_ae[imputed_transit_type == 'access',
  `:=`(
    is_linked_access = 1, is_linked_egress = 0,
    is_imputed_access = 1)]

trip_with_ae[imputed_transit_type == 'egress',
  `:=` (
    is_linked_egress = 1, is_linked_access = 0,
    is_imputed_egress = 1)]

trip_with_ae[is_transit == 0,
  c('is_linked_access', 'is_linked_egress') := NA]

trip_with_ae[, imputed_transit_type := NULL]

trip_with_ae[is_transit == 1, `:=`(
  has_linked_access = sum(is_linked_access, na.rm=TRUE),
  has_linked_egress = sum(is_linked_egress, na.rm=TRUE)),
  by=.(linked_trip_id)]

trip_with_ae[, c('is_first', 'is_last', 'max_trip_id') := NULL]

setcolorder(trip_with_ae, c(
  'hh_id', 'person_id', 'person_num', 'day_num', 'day_complete', 
  'trip_id', 'rm_trip_id', 'trip_num', 'linked_trip_id', 'leg_num', 'n_legs',
  'depart_time_imputed', 'arrive_time', 'dwell_time_min', 'duration_imputed', 'speed_mph_imputed',
  'o_lon', 'o_lat', 'd_lon', 'd_lat', 'distance', 'o_inregion', 'd_inregion',
  'o_taz', 'd_taz', 'o_county_fips', 'd_county_fips', 'o_bg_geo_id', 'd_bg_geo_id',
  'o_purpose_imputed', 'd_purpose_imputed',
  'o_purpose_category_imputed', 'd_purpose_category_imputed',
  'mode_type_imputed', 'rep_access_recode', 'rep_egress_recode',
  'is_transit', 'has_linked_access', 'has_linked_egress', 
  'has_synthetic_access', 'has_synthetic_egress', 
  'is_linked_access', 'is_linked_egress', 'is_imputed_access', 'is_imputed_egress',
  'survey_complete_time', 'survey_complete_trip'))
  
# d_purpose_imputed of access trips
stopifnot(
  
  all(trip_with_ae[is_imputed_access == 1, d_purpose_imputed] == purpose_change_mode),
  
  all(trip_with_ae[is_imputed_egress == 1, o_purpose_imputed] == purpose_change_mode),
  
  all(trip_with_ae[has_synthetic_access == 1 & leg_num == 1, d_purpose_imputed] == purpose_change_mode),
  all(trip_with_ae[has_synthetic_access == 1 & leg_num == 2, o_purpose_imputed] == purpose_change_mode),
  
  all(trip_with_ae[has_synthetic_egress == 1 & leg_num == n_legs, o_purpose_imputed] == purpose_change_mode),
  all(trip_with_ae[has_synthetic_egress == 1 & leg_num == (n_legs - 1), d_purpose_imputed] == purpose_change_mode),
  
  all(trip_with_ae[has_linked_access == 1 & leg_num == 1, is_linked_access] == 1),
  all(trip_with_ae[has_linked_access == 1 & leg_num > 1, is_linked_access] == 0),
  
  all(trip_with_ae[has_linked_egress == 1 & leg_num == n_legs, is_linked_egress] == 1),
  all(trip_with_ae[has_linked_egress == 1 & leg_num < n_legs, is_linked_egress] == 0))

# Make sure flags are defined properly depending on is_transit
stopifnot(
  all(trip_with_ae[is_linked_access == 1, is_transit_leg] == 0),
  all(trip_with_ae[is_linked_egress == 1, is_transit_leg] == 0),
  all(trip_with_ae[is_transit == 1, !is.na(has_linked_access)]),
  all(trip_with_ae[is_transit == 0, is.na(has_linked_access)]),
  all(trip_with_ae[is_transit == 1, !is.na(has_linked_egress)]),
  all(trip_with_ae[is_transit == 0, is.na(has_linked_egress)]),
  
  all(trip_with_ae[is_transit == 1, !is.na(has_synthetic_access)]),
  all(trip_with_ae[is_transit == 0, is.na(has_synthetic_access)]),
  all(trip_with_ae[is_transit == 1, !is.na(has_synthetic_egress)]),
  all(trip_with_ae[is_transit == 0, is.na(has_synthetic_egress)])
)

# Make sure all transit trips have access and egress
stopifnot(
  
  all(trip_with_ae[is_transit == 1, has_linked_access] == 1),
  all(trip_with_ae[is_transit == 1, has_linked_egress] == 1)

)

# add quality flag
message('Adding quality flags')

trip_with_ae[is_imputed_access == 1, transit_quality_flag := 'SA'] # don't need to paste0 since these are brand new trips/don't already have a quality flag
trip_with_ae[is_imputed_egress == 1, transit_quality_flag := 'SE']

setdiff(trip_delivered_cols, names(trip_with_ae))

remove_cols = c('linked_trip_num', 'is_imputed_access', 'is_imputed_egress') # remove this since we should use linked_trip_id
trip_with_ae[, (remove_cols) := NULL]


# Set highest priority mode


# Pick "highest priority" mode from mode_1 thru mode_4 using ranking

if ( region == 'bayarea' ){
  
  trip_modes = melt(trip_with_ae[, .(trip_id, linked_trip_id, mode_1, mode_2, mode_3, mode_4)], 
    id.vars = c('trip_id', 'linked_trip_id')) %>% 
    .[value != missing_skip_value]
} else {
  
  trip_modes = melt(trip_with_ae[, .(trip_id, linked_trip_id, mode_1, mode_2, mode_3)], 
    id.vars = c('trip_id', 'linked_trip_id')) %>% 
    .[value != missing_skip_value]
}

trip_modes = trip_modes[mode_priority, on = .(value)]

trip_modes_highest = trip_modes[trip_modes[, .I[which.max(importance)], by = .(trip_id)]$V1]

trip_with_ae[trip_modes_highest, mode_priority := i.value, on = .(trip_id)]
trip_with_ae[is.na(mode_priority), mode_priority := mode_1] # if missing

stopifnot("NAs in mode_priority" = all(!is.na(trip_with_ae[, mode_priority])))
stopifnot("mode_priority value not in codebook" = 
            all(trip_with_ae[, mode_priority %in%  codebook[variable == 'mode_1', c(value, missing_non_response_value, missing_skip_value)]]))


transit_modes = trip_modes[value %in% c(mode_code_bus, mode_code_rail, mode_code_ferry, mode_code_intercity_rail)]

transit_modes_highest = transit_modes[transit_modes[, .I[which.max(importance)], by = .(linked_trip_id)]$V1]

trip_with_ae[transit_modes_highest, mode_priority_linked := i.value, on = .(linked_trip_id)]
trip_with_ae[is.na(mode_priority_linked), mode_priority_linked := mode_priority]

# Number highest priority leg -- first leg with highest priority mode
trip_with_ae[
  trip_with_ae[mode_priority_linked == mode_priority, .(leg_num = min(leg_num)), by = .(linked_trip_id)], 
  priority_leg_num := i.leg_num, on = .(linked_trip_id)]

trip_with_ae[, is_primary_leg := 0]
trip_with_ae[leg_num == priority_leg_num | n_legs == 1, is_primary_leg := 1]

# Add access/egress mode type 
trip_with_ae[trip_with_ae[is_linked_access == 1], access_mode_type := i.mode_type_imputed, on = .(linked_trip_id)]
trip_with_ae[trip_with_ae[is_linked_egress == 1], egress_mode_type := i.mode_type_imputed, on = .(linked_trip_id)]

# Set NA to missing_skip value on access/egress flags
cols = c('is_linked_egress', 'is_linked_access', 'has_linked_egress', 
         'has_linked_access', 'has_synthetic_egress', 'has_synthetic_access',
         'access_mode_type', 'egress_mode_type')

trip_with_ae[, (cols) := lapply(.SD, nafill, fill = missing_skip_value), 
             .SDcols = cols]

# Fix duration/speed imputation issue in previously delivered dataset
trip_with_ae[, duration_imputed := 
               interval(depart_time_imputed, arrive_time)/seconds(1)/60]

trip_with_ae[, speed_mph_imputed := 
               distance/(interval(depart_time_imputed, arrive_time)/seconds(1)/3600)]

# fix negative values from DST
trip_with_ae[duration_imputed < 0, duration_imputed := 60 + (interval(depart_time_imputed, arrive_time)/seconds(1)/60)]
trip_with_ae[speed_mph_imputed < 0, speed_mph_imputed := distance/(60 + (interval(depart_time_imputed, arrive_time)/seconds(1)/3600))]

# Round distances and speeds to 2 decimals
distance_cols = c('d_distance_home', 'd_distance_work', 'd_distance_school', 'distance',
                  'speed_mph', 'speed_mph_imputed')
trip_with_ae[, (distance_cols) := 
              lapply(.SD, function(col) round(col, 2)), 
            .SDcols = distance_cols]

# Round duration to 0 decimals
duration_cols = c('duration', 'duration_imputed',  'dwell_time_min')
trip_with_ae[, (duration_cols) := 
              lapply(.SD, function(col) round(col, 1)), 
            .SDcols = duration_cols]


# Write data -------------------------------------------------------------------

message('Writing out corrected transit data')

path = file.path(work_dir, paste0('trip_transit_corrected_', region, '.rds'))
saveRDS(trip_with_ae, path)
fwrite(trip_with_ae, file.path(work_dir, paste0('trip_transit_corrected_', region, '.csv')))


