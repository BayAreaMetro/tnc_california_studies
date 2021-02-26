# Transit trip relinking
# 2 Feb 2021 RSG, Inc.

# Code assumes working directory is location of this file

# Set region to work on
# region = 'bayarea'
# region = 'sandag'
# region = 'scag'

message('region is ', region)

# Load libraries -------------------------------------------------------------

necessary_packages = c("data.table", "stringr", "magrittr")

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

# Codebook

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

# # Crosswalk between mode_type_imputed, and reported mode
# # 
# # Following code is to paste values into Excel to create the crosswalk
# # between rail/bus access/egress and mode_type/mode_type_imputed
#
# codebook[variable == 'bus_access', .(value, label_mtc, label_sandag, label_scag)] %>% 
#   View()
#
# codebook[variable == 'bus_access', .(value, label_long)] %>%
#   write.table('clipboard', sep='\t', row.names=FALSE)
# 
# codebook[variable == 'mode_type_imputed', .(value, label_long)] %>%
#   write.table('clipboard', sep='\t', row.names=FALSE)

xwalk = fread(
  file = file.path(
    data_dir, 'mode_type_crosswalk_transit_to_pipeline.csv'))

missing_skip_value = codebook[
  variable == '_All categorical variables_' &
    label_long == "Missing: Skip logic", value]

# Load data -------------------------------------------------------------------

# Load rm_trip_id -- a trip ID prior to unlinking. Allows re-linking unlinked trips

rm_trip_ids = fread(file.path(data_dir, paste0(region, '_rm_trip_id.csv'))) # RSG will provide this to agencies

# Load trip file with updated imputed modes 
trip = readRDS(file.path(work_dir, paste0('trip_new_numtravelers_', region, '.rds')))
day = fread(file.path(tsv_dir, 'ex_day.tsv'))

n_trip = nrow(trip)

trip = merge(
  trip,
  rm_trip_ids, 
  by = c('trip_id'),
  all.x=TRUE
)

stopifnot(
  n_trip == nrow(trip),
  trip[is.na(rm_trip_id), .N] == 0
)


# Merge in day for day_complete

# Check for mismatches
mm = trip[!day, on=c('person_id', 'day_num')]
mm[, .N]
mm[, .N, day_num] # All the mismatches are day_num = 0

trip = merge(
  trip,
  day[, .(person_id, day_num, day_complete)],
  by = c('person_id', 'day_num'),
  all.x=TRUE)

stopifnot(
  n_trip == nrow(trip),
  trip[day_num > 0 & is.na(day_complete), .N] == 0
  )

trip[is.na(day_complete), day_complete := 0]


# Identify out of region trips

if ( region == 'bayarea' ){
  in_counties = c('001', '013', '041', '055', '075', '081', '085', '095', '097')
} else if ( region == 'sandag' ){
  in_counties = c('073')
} else if ( region == 'scag' ){
  in_counties = c('037', '059')
} 

trip[, o_inregion := 1 * (o_county_fips %in% as.numeric(in_counties))]
trip[, d_inregion := 1 * (d_county_fips %in% as.numeric(in_counties))]

mode_type_ld = codebook[variable == 'mode_type' &
    label_long %like% 'Long-distance', value]

mode_type_transit = codebook[variable == 'mode_type' &
    label_long %like% 'Transit', value]


# Update mode assignment ---------------------------------------------------
# Intercity rail in region to be transit, not LD_passenger

mode_code_ic_rail = codebook[variable == 'mode_1' &
    label_short == 'Intercity rail', value]

# settings$mode_category_values$ld_passenger
mode_code_ld = codebook[variable == 'mode_1' &
    label_long %like% 'Intercity bus|Airplane', value]

mode_type_ld = codebook[variable == 'mode_type' &
    label_long %like% 'Long-distance', value]

mode_type_transit = codebook[variable == 'mode_type' &
    label_long %like% 'Transit', value]

# Where mode is intercity rail, assign transit, not LD passenger
trip[, mode_type_old := mode_type]
trip[, mode_type_imputed_old := mode_type_imputed]


# Reassign mode type if any modes are intercity rail but NOT if any are LD passenger

if ( region == 'bayarea' ){

  ic_rail_trips =
    trip[
      (o_inregion == 1 & d_inregion == 1) &
        (mode_1 == mode_code_ic_rail |
            mode_2 == mode_code_ic_rail |
            mode_3 == mode_code_ic_rail |
            mode_4 == mode_code_ic_rail) &
        (!mode_1 %in% mode_code_ld &
            !mode_2 %in% mode_code_ld &
            !mode_3 %in% mode_code_ld &
            !mode_4 %in% mode_code_ld), trip_id]

} else {

  # SANDAG and SCAG use three modes, not four

  ic_rail_trips =
    trip[
      (o_inregion == 1 & d_inregion == 1) &
        (mode_1 == mode_code_ic_rail |
            mode_2 == mode_code_ic_rail |
            mode_3 == mode_code_ic_rail) &
        (!mode_1 %in% mode_code_ld &
            !mode_2 %in% mode_code_ld &
            !mode_3 %in% mode_code_ld), trip_id]

}

trip[trip_id %in% ic_rail_trips,
  `:=`(
    mode_type = mode_type_transit,
    mode_type_imputed = mode_type_transit)]

trip[mode_type_old != mode_type, .N]
trip[mode_type_imputed_old != mode_type_imputed, .N]

trip[, mode_type_old := NULL]
trip[, mode_type_imputed_old := NULL]

# Fix duration imputed (problem with small # of trips in original data)

trip[, duration_imputed := 
       round(as.numeric(difftime(arrive_time, depart_time_imputed, units = 'secs'))/60, 1)]
trip[, duration := round(duration, 1)]


# New linked trips ============================================================

# Combine bus and rail access/egress into a single variable --------------------
#
# Using:
# 
#   - bus_access if it is not missing and rail_access is missing,
#   - rail_access if it is not missing and bus_access is missing,
#   - bus_access if mode_1 is 28 Other bus, 46 Local bus, 55 Express bus
#   - rail_access if mode_1 is 30 BART, 39 Light rail, 42 Other rail, 68 Cable car
#   - NA otherwise (i.e. mode_1 is not in that list and both bus and rail are non-missing
# 
# (same for egress)

mode_code_bus = c(28, 46, 55)
mode_code_rail = c(30, 39, 42, 68)

bus_modes = switch(region,
  bayarea = mode_code_bus,
  sandag = mode_code_bus,
  scag = mode_code_bus)

rail_modes = switch(region,
  bayarea = mode_code_rail,
  sandag = mode_code_rail,
  scag = mode_code_rail)

# Combine bus and rail access modes

trip[, rep_access_mode := fcase(
  (abs(bus_access) < 990 & abs(rail_access) > 990), bus_access,
  (abs(rail_access) < 990 & abs(bus_access) > 990), rail_access,
  mode_1 %in% bus_modes, bus_access,
  mode_1 %in% rail_modes, rail_access,
  default=NA
)]

trip[, rep_egress_mode := fcase(
  (abs(bus_egress) < 990 & abs(rail_egress) > 990), bus_egress,
  (abs(rail_egress) < 990 & abs(bus_egress) > 990), rail_egress,
  mode_1 %in% bus_modes, bus_egress,
  mode_1 %in% rail_modes, rail_egress,
  default=NA
)]


# Crosswalk between mode_type_imputed, and reported mode

trip_new = merge(trip, 
  xwalk[, .(
    rep_access_mode,
    rep_access_recode = mode_type_imputed)],
  by = 'rep_access_mode', all.x=TRUE) 

trip_new = merge(trip_new,
  xwalk[, .(
    rep_egress_mode = rep_access_mode,
    rep_egress_recode = mode_type_imputed)],
  by = 'rep_egress_mode', all.x=TRUE)

trip_new[is.na(rep_access_recode), rep_access_recode := missing_skip_value]
trip_new[is.na(rep_egress_recode), rep_egress_recode := missing_skip_value]
stopifnot(nrow(trip) == nrow(trip_new))


# Define transit legs -------------------------------------------

trip_new[, is_transit_leg := 1 * (mode_type_imputed == mode_type_transit)]

(n_transit_legs = trip_new[, .N, is_transit_leg])


# Assign linking_type ------------------------------------------------------

# Linking class > 0 means trip should be linked to previous trip

# Set parameters

p_cat_change_mode = codebook[
  variable == 'd_purpose_category' &
    label_long == 'Change mode', value]

p_code_change_mode = codebook[
  variable == 'd_purpose' &
    label_long %like% 'Change/transfer', value]

relinked_impute_type = 40 # purpose impute type when linked & set to change mode

max_transit_dwell = 60

max_nontransit_dwell = 30

max_same_purpose_dwell = 15

max_egress_duration = 15

max_egress_dwell = 10

mode_cat_walk = codebook[variable == 'mode_type' & label_long %like% 'Walk', value]

setorder(trip_new, person_id, depart_time_imputed)

# Set default value
max_linking_type = 0L
trip_new[, linking_type := max_linking_type]

linking_type_labels = data.table(value = 0, label = 'No link')

message(
  'Linking type: ', max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])


# previously split in unlinking

max_linking_type = max_linking_type + 1
trip_new[
  !is.na(unlinked_split) &
    unlinked_split == 1 &
    !(get_prev(is_transit_leg) == 0 & is_transit_leg == 0) &
    get_prev(rm_trip_id) == rm_trip_id &
    get_prev(person_id) == person_id,
  linking_type := max_linking_type]

linking_type_labels = rbind(
  linking_type_labels,
  data.table(value = max_linking_type, label = 'Unlinked'))

message(
  'Linking type: ',max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])


# access or transit: leg with change mode purpose followed by transit leg
max_linking_type = max_linking_type + 1

trip_new[
  linking_type == 0 &
    is_transit_leg == 1 &
    (get_prev(d_purpose_imputed) %in% p_code_change_mode) &
    (get_prev(dwell_time_min) <= max_transit_dwell) &
    (get_prev(person_id) == person_id),
  linking_type := max_linking_type]

linking_type_labels = rbind(
  linking_type_labels,
  data.table(value = max_linking_type, label = 'A: change mode'))

message(
  'Linking type: ', max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])


# egress: transit leg with change mode purpose followed by non-transit leg
max_linking_type = max_linking_type + 1

trip_new[
  linking_type == 0 &
  is_transit_leg == 0 &
    get_prev(is_transit_leg) == 1 &
    get_prev(d_purpose_imputed) %in% p_code_change_mode &
    get_prev(dwell_time_min) <= max_nontransit_dwell &
    get_prev(person_id) == person_id,
  linking_type := max_linking_type]

linking_type_labels = rbind(
  linking_type_labels,
  data.table(value = max_linking_type, label = 'E: change mode'))

message(
  'Linking type: ', max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])


#  repeated reported purpose (for erroneous reported purpose)
# NOTE: this has to use *reported* purpose, not imputed purpose
max_linking_type = max_linking_type + 1

trip_new[
  linking_type == 0 &
    get_prev(is_transit_leg) == 1 & 
    get_prev(d_purpose) == d_purpose &
    get_prev(dwell_time_min) <= max_same_purpose_dwell &
    get_prev(person_id) == person_id,
  linking_type := max_linking_type]

linking_type_labels = rbind(
  linking_type_labels,
  data.table(value = max_linking_type, label = 'Same reported purpose'))

message(
  'Linking type: ', max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])


# previous *reported* mode matches reported_access_mode
max_linking_type = max_linking_type + 1

trip_new[
  linking_type == 0 &
  is_transit_leg == 1 & 
    get_prev(is_transit_leg) == 0 &
    !is.na(rep_access_recode) &
    get_prev(mode_type) == rep_access_recode & 
    get_prev(dwell_time_min) <= max_transit_dwell &
    get_prev(person_id) == person_id, 
  linking_type := max_linking_type]

linking_type_labels = rbind(
  linking_type_labels,
  data.table(value = max_linking_type, label = 'A: rep. mode matches rep. access'))

message(
  'Linking type: ', max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])



# previous imputed mode matches reported_access_mode
max_linking_type = max_linking_type + 1

trip_new[
  linking_type == 0 &
    is_transit_leg == 1 & 
    get_prev(is_transit_leg) == 0 &
    !is.na(rep_access_recode) &
    get_prev(mode_type_imputed) == rep_access_recode & 
    get_prev(dwell_time_min) <= max_transit_dwell &
    get_prev(person_id) == person_id, 
  linking_type := max_linking_type]

linking_type_labels = rbind(
  linking_type_labels,
  data.table(value = max_linking_type, label = 'A: imp. mode matches rep. access'))

message(
  'Linking type: ', max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])



# *reported* mode matches previous reported_egress_mode
max_linking_type = max_linking_type + 1

trip_new[
  linking_type == 0 &
  is_transit_leg == 0 &
    get_prev(is_transit_leg) == 1 &
    !is.na(get_prev(rep_egress_recode)) &
    mode_type == get_prev(rep_egress_recode) & 
    get_prev(dwell_time_min) <= max_nontransit_dwell &
    get_prev(person_id) == person_id,
  linking_type := max_linking_type]

linking_type_labels = rbind(
  linking_type_labels,
  data.table(value = max_linking_type, label = 'E: rep. mode matches rep. egress'))

message(
  'Linking type: ', max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])


# *imputed* mode matches previous reported_egress_mode
max_linking_type = max_linking_type + 1

trip_new[
  linking_type == 0 &
    is_transit_leg == 0 &
    get_prev(is_transit_leg) == 1 &
    !is.na(get_prev(rep_egress_recode)) &
    mode_type_imputed == get_prev(rep_egress_recode) & 
    get_prev(dwell_time_min) <= max_nontransit_dwell &
    get_prev(person_id) == person_id,
  linking_type := max_linking_type]

linking_type_labels = rbind(
  linking_type_labels,
  data.table(value = max_linking_type, label = 'E: imp. mode matches rep. egress'))

message(
  'Linking type: ', max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])

 
# access, walk trip with short dwell prior to transit
# - trip is transit
# - previous trip is mode = walk
# - previous dwell is short < 30

max_linking_type = max_linking_type + 1

trip_new[
  linking_type == 0 &
    is_transit_leg == 1 &
    get_prev(mode_type_imputed) == mode_cat_walk &
    get_prev(dwell_time_min) <= max_nontransit_dwell &
    get_prev(person_id) == person_id,
  linking_type := max_linking_type]

linking_type_labels = rbind(
  linking_type_labels,
  data.table(value = max_linking_type, label = 'A: walk with short dwell'))

message(
  'Linking type: ', max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])


# egress, walk trip after short dwell
# - imputed mode is walk
# - follows transit trip
# - dwell is < 30 min

max_linking_type = max_linking_type + 1

trip_new[
  linking_type == 0 &
    is_transit_leg == 0 &
    get_prev(is_transit_leg) == 1 &
    mode_type_imputed == mode_cat_walk &
    get_prev(dwell_time_min) <= max_nontransit_dwell &
    get_prev(person_id) == person_id,
  linking_type := max_linking_type]

linking_type_labels = rbind(
  linking_type_labels,
  data.table(value = max_linking_type, label = 'E: walk after short dwell'))

message(
  'Linking type: ', max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])


# egress, short trip after short dwell
# - trip duration is <= 15 min
# - previous dwell is <= 30 min
# - follows a transit trip

max_linking_type = max_linking_type + 1

trip_new[
  linking_type == 0 &
    is_transit_leg == 0 &
    get_prev(is_transit_leg) == 1 &
    get_prev(dwell_time_min) <= max_nontransit_dwell &
    duration_imputed <= max_egress_duration &
    get_prev(person_id) == person_id,
  linking_type := max_linking_type]

linking_type_labels = rbind(
  linking_type_labels,
  data.table(value = max_linking_type, label = 'E: short trip after short dwell'))

message(
  'Linking type: ', max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])


# egress with short dwell
# - non-transit trip follows transit trip with very short dwell

max_linking_type = max_linking_type + 1

trip_new[
  linking_type == 0 &
    is_transit_leg == 0 &
    get_prev(is_transit_leg) == 1 &
    get_prev(dwell_time_min) <= max_egress_dwell &
    get_prev(person_id) == person_id,
  linking_type := max_linking_type]

linking_type_labels = rbind(
  linking_type_labels,
  data.table(value = max_linking_type, label = 'E: non-transit after transit with short dwell'))

message(
  'Linking type: ', max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])


# consecutive transit trips with short dwell between (<= 15 mins)
# UNLESS they have purpose of
# 6 - Shop, 7 - Meal, 8 - Social/recreation, 9 - Errand/appointment
purpose_category_allowed = c(6, 7, 8, 9)

max_linking_type = max_linking_type + 1

trip_new[
  linking_type == 0 &
    is_transit_leg == 1 &
    get_prev(is_transit_leg) == 1 &
    !(get_prev(d_purpose_category_imputed) %in% purpose_category_allowed) &
    get_prev(dwell_time_min) <= max_same_purpose_dwell &
    get_prev(person_id) == person_id,
  linking_type := max_linking_type]

linking_type_labels = rbind(
  linking_type_labels,
  data.table(
    value = max_linking_type,
    label = 'Consecutive transit trips with short dwell')
  )

message(
  'Linking type: ', max_linking_type,
  '; n = ', trip_new[linking_type == max_linking_type, .N])


## Don't link trips where reported & imputed purpose is home/work/school

habitual_loc_purpose_category = c(1, 2, 4)

trip_new[
  linking_type != 0 &
    get_prev(d_purpose_category_imputed) %in% habitual_loc_purpose_category &
    get_prev(d_purpose_category_imputed) == get_prev(d_purpose_category) &
    get_prev(person_id) == person_id,
  linking_type := 0]


trip_new[linking_type > 0,
  .(N = .N, pct=(.N / trip_new[linking_type > 0, .N])),
  keyby=linking_type]


# Finished assigning linking types

# Write out linking type labels for reference
label_path = file.path(work_dir, 'linking_type_labels.csv')
if ( !file.exists(label_path) ){
  fwrite(linking_type_labels, label_path)
}

# Assign a linked ID -------------------------------------------------------
# Where linking type > 0, assign linked_trip_id to that of previous trip

# Create linked_trip_id
message('Assigning linked ID')

setorder(trip_new, person_id, depart_time_imputed)
trip_new[, linked_trip_id := 1:nrow(trip_new)]
trip_new_ref = rep(1, times=nrow(trip_new)) 
# Compare to linked trips so we can stop when it is stable.

iter = 1
while (any(trip_new_ref != trip_new[, linked_trip_id])) {
  
  message(paste("Linking iteration:", iter))
  trip_new_ref = trip_new[, linked_trip_id]
  
  trip_new[,
    linked_trip_id := ifelse(
      linked_trip_id > 1L & linking_type >= 1L & get_prev(day_num) == day_num,
      get_prev(linked_trip_id), 
      linked_trip_id), 
    by = .(person_id)]

  iter = iter + 1
}

message('Cleaning up linked_trip_id')

# Collapse linked ID to be sequential
trip_new[,
  linked_trip_id := frank(linked_trip_id, ties.method = "dense")]

# Reassign leg_num to be sure they are sequential within linked_trip_id
trip_new[, leg_num := seq_len(.N), by = .(linked_trip_id)]


# Flag access/egress legs -----------------------------------------------------
message('Creating trip flags')

trip_new[, n_legs := .N, by = linked_trip_id]

# Identify transit trips 
transit_grps = trip_new[
  mode_type_imputed == mode_type_transit & (o_inregion == 1 & d_inregion == 1), 
  linked_trip_id] %>% unique()

trip_new[, is_transit := 1 * (linked_trip_id %in% transit_grps)]
trip_new[is_transit == 1, unique(linked_trip_id)] %>% length()  
trip_new[is_transit == 1, .N] 

trip_new[is_transit_leg == 1 & is_transit == 0, .N]  # These are out of region
trip_new[is_transit_leg == 1 & is_transit == 0, is_transit_leg := 0]

setorder(trip_new, person_id, depart_time_imputed)

# access
trip_new[is_transit == 1,
  is_linked_access := 1 * (
    n_legs > 1 &
      leg_num == 1 &
      is_transit_leg == 0 &
      get_next(is_transit_leg) == 1 ),
  by = .(linked_trip_id)]  

# egress
trip_new[is_transit == 1,
  is_linked_egress := 1 * (
    n_legs > 1 &
      leg_num == max(leg_num) &
      is_transit_leg == 0 & 
      get_prev(is_transit_leg) == 1), 
  by = .(linked_trip_id)]

trip_new[is_transit == 1, .N, is_linked_access]
trip_new[is_transit == 1, .N, is_linked_egress]

trip_new[is_transit == 1, `:=`(
  has_linked_access = sum(is_linked_access, na.rm=TRUE),
  has_linked_egress = sum(is_linked_egress, na.rm=TRUE)),
  by=.(linked_trip_id)]


# Edit purpose imputed to change mode 
trip_new[, fmr_purpose_imputed := d_purpose_imputed]

trip_new[get_next(linking_type) > 0 & 
                 get_next(person_id) == person_id,
               `:=` (d_purpose_imputed = p_code_change_mode,
                     d_purpose_category_imputed = p_cat_change_mode,
                     d_purpose_impute_type = relinked_impute_type)
                 ]

trip_new[linking_type > 0 & 
                 get_prev(person_id) == person_id,
               `:=` (o_purpose_imputed = p_code_change_mode,
                     o_purpose_category_imputed = p_cat_change_mode,
                     o_purpose_impute_type = relinked_impute_type)
]

# Add quality flag ---------------------------------------------------------

message('Adding quality flags')

# key:
# SA = is synthetic access (added later)
# SE = is synthetic egress (added later)
# LD = access/egress is long distance passenger mode (e.g., airplane, intercity bus)
# MODE = access/egress mode does not match reported access/egress mode (for next/previous trip)
# other?


quality_flag_key = data.table(
  values = c('SA', 'SE', 'LD', 'MODE'), 
  labels = c('Is synthetic access',
    'Is synthetic egress',
    'Access/egress is LD passenger mode',
    'Access/egress mode != reported access/egress mode'))

trip_new[, transit_quality_flag := NA_character_]
trip_new[, transit_quality_flag := fcase(
  is_linked_access == 1 & mode_type_imputed == mode_type_ld,  'LD',
  is_linked_egress == 1 & mode_type_imputed == mode_type_ld, 'LD'), by = .(linked_trip_id)]

trip_new[, transit_quality_flag :=  fcase(
  is_linked_access == 1 &
    get_next(rep_access_recode) != mode_type_imputed &
    !is.na(get_next(rep_access_recode)),
  paste0(c(na.omit(transit_quality_flag), 'MODE'), collapse = ','),
  is_linked_egress == 1 & 
    get_prev(rep_egress_recode) != mode_type_imputed &
    !is.na(get_prev(rep_egress_recode)),
  paste0(c(na.omit(transit_quality_flag), 'MODE'), collapse = ',')),
  by = .(linked_trip_id)]


# Write to file  -------------------------------------------------------------

saveRDS(trip_new, file.path(work_dir, paste0('trip_transit_relinked_', region, '.rds')))

message('Finished relinking')
