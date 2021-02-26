# Linked trip table generation
# 17 Feb 2021 RSG, Inc.


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


# Load settings ---------------------------------------------------------------

# Load codebook

codebook_label_col = switch(region,
                            bayarea = 'label_mtc',
                            sandag = 'label_sandag',
                            scag = 'label_scag')

codebook = read_codebook(codebook_path)
setnames(codebook, codebook_label_col, 'label_long')
codebook[, label_short := label_long]
codebook[, label_short := str_replace(label_short, 'Missing: ', '')]
codebook[, label_short := str_replace(label_short, ' [(].*[)]$', '')]
codebook[, label_short := str_trunc(label_short, width=20, side='right')]
codebook[, value_label := paste(value, label_short)]
codebook = codebook[, .(variable, value, label_long, value_label, label_short)]

variable_labels = read_codebook(codebook_path, varvals=FALSE)
variable_labels = variable_labels[, .(variable, data_type)]

mode_type_ld = codebook[variable == 'mode_type' &
                          label_long %like% 'Long-distance', as.integer(value)]

mode_type_transit = codebook[variable == 'mode_type' &
                               label_long %like% 'Transit', as.integer(value)]
missing_values= codebook[
  variable == '_All categorical variables_' &
    label_long %like% "Missing", as.integer(value)]

missing_non_response_value = codebook[
  variable == '_All categorical variables_' &
    label_long == "Missing: Non-response", as.integer(value)]

missing_skip_value = codebook[
  variable == '_All categorical variables_' &
    label_long == "Missing: Skip logic", as.integer(value)]


mode_code_bus = c(28, 46, 55)
mode_code_rail = c(30, 39, 42, 68)
mode_code_ferry = 32
mode_code_intercity_rail = 41

mode_priority = fread(file.path(data_dir, 'mode_importance_ranking.csv'))


# Load data ------------------------------------------------------------------

path = file.path(work_dir, paste0('trip_transit_corrected_', region, '.rds'))

trip = readRDS(path)


# Linked trip processing ------------------------------------------------------------------


message("Pre-processing unlinked trip data")
trip = trip[order(person_id, trip_num, depart_time_imputed, arrive_time)] # make sure chronological order


# Flag first/last non-synthetic

trip[!transit_quality_flag %in% c('SA', 'SE'), `:=` (is_first_real = leg_num == min(leg_num),
                                                     is_last_real = leg_num == max(leg_num)), 
     by = .(linked_trip_id)]

trip[is.na(is_first_real), is_first_real := FALSE]
trip[is.na(is_last_real), is_last_real := FALSE]

trip[, is_last := leg_num == max(leg_num), by = .(linked_trip_id)]

stopifnot("Trip doesn't have flagged first real leg" = 
            length(unique(trip$linked_trip_id)) == trip[is_first_real == TRUE, .N])

stopifnot("Trip doesn't have flagged last real leg" = 
            length(unique(trip$linked_trip_id)) == trip[is_last_real == TRUE, .N])

# Get imputed dwell time min
trip = trip[order(person_id, depart_time_imputed, depart_time, arrive_time)]
trip[, next_depart_imputed := get_next(depart_time_imputed), by = .(person_id)]

# for this purpose, we only care about dwell til the next trip; not dwell at end of day

trip[, dwell_time_imputed := as.numeric(difftime(next_depart_imputed, arrive_time, units = "mins"))]
trip[is.na(dwell_time_imputed), dwell_time_imputed := 0]
trip[dwell_time_imputed < 0, dwell_time_imputed := 0]
# Flag survey answers for first relevant trip

survey_variables = c('driver', 
                     'park_location', 
                     'park_type',
                     'mode_uber', 
                     'mode_lyft', 
                     'tnc_pooled',
                     'tnc_schedule',
                     'tnc_decide',
                     'tnc_wait',
                     'tnc_wait_time',
                     'taxi_type_payment',
                     'taxi_pay',
                     'taxi_cost',
                     'tnc_promo',
                     'tnc_replace',
                     'bus_type_payment',
                     'rail_type_payment',
                     'bike_park_location',
                     'scooter_park_location')

# Create reverse ordered table to get 1st answered record by joining on linked trip ID

trip_rev = copy(trip[order(person_id, -trip_num, -depart_time_imputed, -arrive_time)]) 

# setting "first_<variable>" to the first answered value 
# the join picks the last value that matches the join field, thus the need for the reversed table

for(variable in survey_variables) {
  trip[trip_rev[!get(variable) %in% missing_values],
       (paste0('first_', variable)) := get(paste0('i.', variable)),
       on = .(linked_trip_id)]
  
  # Set to missing_skip_value if missing
  trip[is.na(get(paste0('first_', variable))), (paste0('first_', variable)) := missing_skip_value]
}

# get first survey complete time
trip[trip[is_first_real == TRUE], first_real_complete_time := i.survey_complete_time, on = .(linked_trip_id)]
trip[trip[leg_num == 1], first_o_purpose_other := i.o_purpose_other, on = .(linked_trip_id)]
trip[trip[is_last == TRUE], last_d_purpose_other := i.d_purpose_other, on = .(linked_trip_id)]

# get duration in seconds to avoid infinite speeds

trip[, duration_sec := interval(depart_time, arrive_time)/seconds(1)]
trip[, duration_imputed_sec := interval(depart_time_imputed, arrive_time)/seconds(1)]

# handle DST (11/04)
trip[duration_sec < 0, duration_sec := duration_sec + 3600]
trip[duration_sec < 0, duration_imputed_sec := duration_imputed_sec + 3600]



# Get transit priority modes in order for linked trip modes
message("Getting transit mode sequence")

trip[is_linked_access == 0 & is_linked_egress == 0 & mode_type %in% c(mode_type_ld, mode_type_transit), 
             prev_is_different := fifelse(get_prev(mode_priority) != mode_priority, 1, 0), by = .(linked_trip_id)]

transit_modes_sequence = melt(trip[(prev_is_different == 1 | is.na(prev_is_different)) & 
                                             is_linked_access == 0 & is_linked_egress == 0 &
                                            mode_type %in% c(mode_type_ld, mode_type_transit)],
                              id.vars = c('linked_trip_id', 'trip_id', 'leg_num'), measure.vars = 'mode_priority')

transit_modes_sequence[, idx := seq_len(.N), linked_trip_id]                              
transit_modes_sequence = dcast(transit_modes_sequence, linked_trip_id ~ variable + idx, value.var = 'value')

# Group unlinked trips by linked ID and choose appropriate value for variable


message('Creating linked trip table.  Please wait')

if ( region == 'sandag' ){
  trip[, hh_member_12 := 995L]
} 

if ( region == 'scag' ){
  missing_hh_members = paste0('hh_member_', 9:12)
  trip[, (missing_hh_members) := 995L]
}

trip_linked = trip[, .(
                       # ID vars: Add most to grouping variable if the same across all legs
                       leg_1_trip_id = max(as.numeric(trip_id) * (leg_num == 1)),
                       # Travel day vars: Use first leg/minimum day
                       day_num =  max(day_num * (leg_num == 1)),
                       day_complete = max(day_complete * (leg_num == 1)), # keep consistent with travel day
                       # Additional variables from relevant leg or grouped sum
                       n_legs = .N,
                       depart_time_imputed = min(depart_time_imputed),
                       depart_time = min(depart_time),
                       arrive_time = max(arrive_time), 
                       dwell_time_min = max(dwell_time_min * (leg_num == max(leg_num))),
                       duration_imputed = sum(duration_imputed, na.rm = TRUE), # ask: include dwell_time_min?
                       duration = sum(duration, na.rm = TRUE), # ask: include dwell_time_min?
                       duration_total = sum(fcoalesce(duration, 0) + (dwell_time_min * (leg_num != max(leg_num))), na.rm = TRUE),
                       duration_total_imputed = sum(fcoalesce(duration_imputed, 0) + 
                                                      (dwell_time_imputed * (leg_num != max(leg_num))), na.rm = TRUE),
                       speed_mph_imputed = 
                         sum(distance, na.rm = TRUE)/
                         (sum(duration_imputed_sec, na.rm = TRUE)/ 3600), # assume don't include dwell_time_min?
                       speed_mph = 
                         sum(distance, na.rm = TRUE)/
                         (sum(duration_sec, na.rm = TRUE) / 3600), # assume don't include dwell_time_min?
                       o_lon = sum(o_lon * (is_first_real), na.rm = TRUE), 
                       o_lat = sum(o_lat * (is_first_real), na.rm = TRUE), 
                       d_lon = sum(d_lon * (is_last_real), na.rm = TRUE), 
                       d_lat = sum(d_lat * (is_last_real), na.rm = TRUE), 
                       distance = sum(distance, na.rm = TRUE),
                       o_inregion = sum(o_inregion * (is_first_real)), 
                       d_inregion = sum(d_inregion * (is_last_real)),
                       o_taz = sum(o_taz * (is_first_real), na.rm = TRUE), 
                       d_taz = sum(d_taz * (is_last_real), na.rm = TRUE),
                       o_county_fips = sum(o_county_fips * (is_first_real), na.rm = TRUE), 
                       d_county_fips = sum(d_county_fips * (is_last_real), na.rm = TRUE),
                       o_bg_geo_id = sum(o_bg_geo_id * (is_first_real), na.rm = TRUE), 
                       d_bg_geo_id = sum(d_bg_geo_id * (is_last_real), na.rm = TRUE),
                       o_purpose_imputed = sum(o_purpose_imputed * (leg_num == 1)),
                       d_purpose_imputed = sum(d_purpose_imputed * (leg_num == max(leg_num))),
                       o_purpose_category_imputed = sum(o_purpose_category_imputed * (leg_num == 1)),
                       d_purpose_category_imputed = sum(d_purpose_category_imputed * (leg_num == max(leg_num))),
                       is_transit = max(is_transit %% missing_skip_value),
                       has_linked_access = max(is_linked_access, na.rm = TRUE),
                       has_linked_egress = max(is_linked_egress, na.rm = TRUE),
                       has_synthetic_access = max(has_synthetic_access, na.rm = TRUE),
                       has_synthetic_egress = max(has_synthetic_egress, na.rm = TRUE),
                       survey_complete_time = max(first_real_complete_time),
                       # Survey complete trip: Take survey completion for the highest priority transit mode
                       survey_complete_trip = sum(survey_complete_trip * is_primary_leg), 
                       rep_access_recode = sum(rep_access_recode * (is_first_real)), # gets renamed later to access_mode_type_reported
                       rep_egress_recode = sum(rep_egress_recode * (is_last_real)), # gets renamed later to egress_mode_type_reported
                       hh_member_1  = max(1  * hh_member_1 == 1),
                       hh_member_2  = max(1  * hh_member_2 == 1),
                       hh_member_3  = max(1  * hh_member_3 == 1),
                       hh_member_4  = max(1  * hh_member_4  == 1),
                       hh_member_5  = max(1  * hh_member_5  == 1),
                       hh_member_6  = max(1  * hh_member_6  == 1),
                       hh_member_7  = max(1  * hh_member_7  == 1),
                       hh_member_8  = max(1  * hh_member_8  == 1),
                       hh_member_9  = max(1  * hh_member_9  == 1),
                       hh_member_10 = max(1 * hh_member_10 == 1),
                       hh_member_11 = max(1 * hh_member_11 == 1),
                       hh_member_12 = max(1 * hh_member_12 == 1),
                       num_hh_travelers = max(num_hh_travelers), # update later
                       num_non_hh_travelers = max(num_non_hh_travelers),
                       num_travelers = max(num_travelers), # update later
                       o_purpose = sum(o_purpose * (leg_num == 1)),
                       o_purpose_other = max(first_o_purpose_other), 
                       d_purpose = sum(d_purpose * (leg_num == max(leg_num))),
                       d_purpose_other = max(last_d_purpose_other),
                       o_purpose_category = sum(o_purpose_category * (leg_num == 1)),
                       d_purpose_category = sum(d_purpose_category * (leg_num == max(leg_num))),
                       access_mode_type = sum((is_linked_access == 1)* mode_type_imputed), # imputed mode type on access leg
                       egress_mode_type = sum((is_linked_egress == 1) * mode_type_imputed), # imputed mode type on egress leg
                       mode_1 = missing_skip_value, # join to mode sequence later
                       mode_2 = missing_skip_value,
                       mode_3 = missing_skip_value,
                       mode_4 = missing_skip_value,
                       mode_type = sum(mode_type * is_primary_leg), # mode type from highest priority leg
                       mode_type_imputed = sum(mode_type_imputed * is_primary_leg), 
                       mode_priority = sum(mode_priority * is_primary_leg),
                       # mode_change_flag: re-set at later step (concatenate flags)
                       # downstream mode questions -- take answer from first leg where mode was used, from above
                       driver = max(first_driver),
                       park_location = max(first_park_location),
                       park_type = max(first_park_type),
                       mode_uber = max(first_mode_uber),
                       mode_lyft = max(first_mode_lyft),
                       tnc_pooled = max(first_tnc_pooled),
                       tnc_schedule = max(first_tnc_schedule),
                       tnc_decide = max(first_tnc_decide),
                       tnc_wait = max(first_tnc_wait),
                       tnc_wait_time = max(first_tnc_wait_time),
                       taxi_type_payment = max(first_taxi_type_payment),
                       taxi_pay = max(first_taxi_pay),
                       taxi_cost = max(first_taxi_cost),
                       tnc_promo = max(first_tnc_promo),
                       tnc_replace = max(first_tnc_replace),
                       bus_type_payment = max(first_bus_type_payment),
                       rail_type_payment = max(first_rail_type_payment),
                       bike_park_location = max(first_bike_park_location),
                       scooter_park_location = max(first_scooter_park_location),
                       # other trip flags: set to 1 if any are 1
                       unlinked_trip = max(unlinked_trip %% missing_skip_value),
                       unlinked_split = max(unlinked_split %% missing_skip_value),
                       user_merged = max(user_merged %% missing_skip_value),
                       user_split = max(user_split %% missing_skip_value),
                       added_trip = max(added_trip %% missing_skip_value),
                       analyst_merged = max(analyst_merged %% missing_skip_value),
                       analyst_split = max(analyst_split %% missing_skip_value),
                       split_due_to_loop = max(split_due_to_loop %% missing_skip_value),
                       #trip_quality_flag: derive later (concatenate from all legs)
                       is_tnc_trip = max(is_tnc_trip %% missing_skip_value),
                       teleport = max(teleport %% missing_skip_value),
                       # Other derived variables - use relevent leg info
                       primary_leg_num = max(priority_leg_num),
                       pickup_distance_m = sum(pickup_distance_m * (is_first_real)),
                       day_num_12am = sum(day_num_12am * (is_first_real)),
                       o_purpose_impute_type = sum(o_purpose_impute_type * (leg_num == 1)),
                       d_purpose_impute_type = sum(d_purpose_impute_type * (leg_num == max(leg_num))),
                       d_distance_home = sum(d_distance_home * (is_last_real)),
                       d_distance_work = sum(d_distance_work * (is_last_real)),
                       d_distance_school = sum(d_distance_school * (is_last_real)),
                       o_location_type = sum(o_location_type * (is_first_real)),
                       d_location_type = sum(d_location_type * (is_last_real)),
                       o_location_type_imputed = sum(o_location_type_imputed * (is_first_real)),
                       d_location_type_imputed = sum(d_location_type_imputed * (is_last_real)),
                       num_travelers_imputed = max(num_travelers_imputed),
                       # omit mode imputation variables
                       # omit weighting variables
                       day_status = sum(day_status * (is_first_real)),
                       linking_type = sum(linking_type * is_primary_leg) # get answer from priority transit leg
                       #transit_quality_flag: derive later (concatenate all flags)
                       ), by = .(hh_id, person_id, person_num, linked_trip_id)]

stopifnot("Linked trip count is different from before" = 
            trip_linked[, .N] == trip[, .N, .(linked_trip_id)][, .N])

# Join modes to transit trips
message("Joining modes to transit trips")

# For non-transit trips, keep modes 1-4 the same as in the unlinked table.
if(!'mode_4' %in% names(trip)){
  trip[, mode_4 := as.integer(missing_skip_value)]
}

modes_non_transit = trip[!linked_trip_id %in% transit_modes_sequence[, linked_trip_id], 
                         .(linked_trip_id, 
                           mode_priority_1 = mode_1, 
                           mode_priority_2 = mode_2, 
                           mode_priority_3 = mode_3, 
                           mode_priority_4 = mode_4)]

transit_modes_sequence = rbindlist(list(transit_modes_sequence, modes_non_transit), use.names = TRUE, fill = TRUE)


trip_linked[transit_modes_sequence, `:=` (mode_1 = fcoalesce(i.mode_priority_1, 995L),
                                                  mode_2 = fcoalesce(i.mode_priority_2, 995L), 
                                                  mode_3 = fcoalesce(i.mode_priority_3, 995L),
                                                  mode_4 = fcoalesce(i.mode_priority_4, 995L)),
                    on = .(linked_trip_id)]

# Ensure transit linked trips don't have non-transit modes -- set to 995 
# Happens if mode_type_imputed is transit but no modes on the trip are transit.

for(mode in c('mode_1', 'mode_2', 'mode_3', 'mode_4')){
  
  trip_linked[n_legs > 1 & !get(mode) %in% 
                c(mode_code_bus, mode_code_rail, mode_code_ferry, mode_code_intercity_rail),
              (mode) := missing_skip_value]
}

# Derive text flags
# Set blanks to NA to ignore in concatenation
message('Deriving text flags')

trip[mode_change_flag == '', mode_change_flag := NA_character_]
trip[trip_quality_flag == '', trip_quality_flag := NA_character_]
trip[trip_quality_flag == missing_skip_value, trip_quality_flag := NA_character_]
trip[transit_quality_flag == '', transit_quality_flag := NA_character_]

# Separate with ; (same as trip quality flag separator)
linked_flags = trip[, lapply(.SD, function(string) paste0(na.omit(string), collapse = ';')), 
                    by = .(linked_trip_id), .SDcols = c('mode_change_flag', 
                                                        'trip_quality_flag', 
                                                        'transit_quality_flag')]

# Join flags to linked trips 

nrows = nrow(trip_linked)

trip_linked = trip_linked[linked_flags, on = .(linked_trip_id)]

stopifnot("Number of rows changed after join" = nrow(trip_linked) == nrows)


# Set num travelers
message('Set num_travelers')

hh_member_cols = names(trip_linked)[names(trip_linked) %like% 'hh_member']
trip_linked[, num_hh_travelers := Reduce(`+`, .SD %% missing_skip_value), .SDcols = hh_member_cols]

# handle missing hh member columns

trip[, traveler_data_missing := max(num_non_hh_travelers), .(linked_trip_id)] # check if all unlinked trips have missing traveler info

# Use info from unlinked table to tell whether data should be present or not 
#  (if householder exists & any unlinked trips have answer)
for(col in hh_member_cols){
  joined_col = paste0('i.', col)
  
  trip_linked[trip, (col) := fifelse(get(joined_col) == missing_skip_value, 
                                     missing_skip_value, get(col)), on = .(linked_trip_id)]
  
}


trip_linked[num_hh_travelers >0 & num_non_hh_travelers >= 0, num_travelers := num_hh_travelers + num_non_hh_travelers]
trip_linked[num_non_hh_travelers < 0, num_travelers := num_non_hh_travelers]
trip_linked[num_non_hh_travelers < 0 & hh_member_2 != missing_skip_value, num_hh_travelers := num_non_hh_travelers]

# if num travelers imputed < num travelers, set to num travelers
trip_linked[num_travelers_imputed < num_travelers, num_travelers_imputed := num_travelers]

# Confirm that when traveler info is not missing (or person 1 is the only householder); 
#  hh travelers is the sum of non-missing householders
stopifnot("Householders don't add up" = 
            nrow(trip_linked[(hh_member_2 == missing_skip_value | num_non_hh_travelers >= 0) & 
                               num_hh_travelers != Reduce(`+`, .SD %% missing_skip_value), 
                             .SDcols = hh_member_cols]) == 0)


# Setting survey answers to the correct missing value
message('Setting missing values for survey answers')

for(col in survey_variables){
  joined_col = paste0('i.', col)
  trip_linked[trip, (col) := fifelse(n_legs == 1, get(joined_col), get(col)), on = .(linked_trip_id)]
  
}

trip_linked[access_mode_type == 0, access_mode_type := missing_skip_value]
trip_linked[egress_mode_type == 0, egress_mode_type := missing_skip_value]

# Duration/distance/speed corrections
message('Duration/distance/speed edits')

# null infinite speed values
trip_linked[is.infinite(speed_mph_imputed), speed_mph_imputed := NA]
trip_linked[is.infinite(speed_mph), speed_mph := NA]

# Round distances and speeds to 2 decimals
distance_cols = c('d_distance_home', 'd_distance_work', 'd_distance_school', 'distance',
                  'speed_mph', 'speed_mph_imputed')
trip_linked[, (distance_cols) := 
              lapply(.SD, function(col) round(col, 2)), 
            .SDcols = distance_cols]

# Round duration to 0 decimals
duration_cols = c('duration', 'duration_imputed', 'duration_total', 'duration_total_imputed', 'dwell_time_min')
trip_linked[, (duration_cols) := 
              lapply(.SD, function(col) round(col, 1)), 
            .SDcols = duration_cols]

# Force consistency with unlinked trip file where n_legs == 1
# (Some are off by a small fraction)

trip_linked[trip, speed_mph := fifelse(n_legs == 1, i.speed_mph, speed_mph), on = .(linked_trip_id)]
trip_linked[trip, speed_mph_imputed := fifelse(n_legs == 1, i.speed_mph_imputed, speed_mph_imputed), on = .(linked_trip_id)]

message('Write linked trip file')
fwrite(trip_linked, file.path(work_dir, paste0('trips_linked_', region, '.csv')))
saveRDS(trip_linked, file.path(work_dir, paste0('trips_linked_', region, '.rds')))
