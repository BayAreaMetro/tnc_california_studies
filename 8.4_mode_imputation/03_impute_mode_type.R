# impute_mode_type.R
# 20 Nov 2019 matt.landis@rsginc.com

# Assign mode_type values for trips that lack it.


# Parameters ---------------------------------------------------------------

# define which study to run
# dbname = "tnc_bayarea"
# dbname = "tnc_sandag"
# dbname = "tnc_scag"

model_version = 'v2.1'

write_results_to_pops = FALSE


# Load libraries -----------------------------------------------------------

library('DBI')
library('data.table')
library('apollo')
library('tmrtools')
library('stringr')
library('sf')
library('tigris')
library('dplyr')
library('janitor')
options(tigris_use_cache = TRUE)
options(tigris_class = 'sf')

Sys.setenv(tz = 'US/Eastern')  # This is the default but make explicit
# Timestamps are stored in PG without timezone, but are actually US/Pacific
# Some conversion happens on reading the table, so timezone has to be 
# Eastern on reading AND writing


# Settings --------------------------------------------------------------------

options(scipen = 99)

message('dbname is ', dbname)

# SharePoint
code_book_path = 'R:/20261_TNC_DatasetImprovements/Data/Consolidated_SB1_TNC_Study_Codebook_27February2020.xlsx'

variable_labels = read_codebook(code_book_path, varvals=FALSE)

label_col = switch(dbname,
                   tnc_bayarea = 'label_mtc',
                   tnc_sandag = 'label_sandag',
                   tnc_scag = 'label_scag')

value_labels = read_codebook(code_book_path, varvals=TRUE, label_col=label_col)

county_fips_in_region = switch(dbname,
                               tnc_bayarea = c("075", "013", "085", "001", "081", "097", "041", "095", "055"),
                               tnc_sandag = c('073'),
                               tnc_scag = c('037', '059'))

stopifnot(!is.null(county_fips_in_region))

mincompletetrips = 1L
maxincompletetrips = 10L

model_dir = file.path(dbname, model_version)
input_functions_path = paste0(model_version, '_input_functions.R')
base_data_path = file.path(model_dir, 'base_data.rds')
predict_data_path = file.path(model_dir, 'predict_data.rds')
model_path = file.path(model_dir, 'model.rds')

model = readRDS(model_path)

source(input_functions_path)   # Gets param_names, among others


# Load data -------------------------------------------------------------------

message('Loading data')
con = connect_to_pops(dbname)

# Test time stamps for correct reading and writing
# Check that times are going be OK by comparison with w_trip_rm in POPS
(test_times = dbGetQuery(con = con, statement = 'select trip_id, depart_time, arrive_time from ex_trip_no_impute order by trip_id limit 5'))

write_to_db(con, test_times, 'test_times', overwrite = TRUE,
            numeric_scale = 13, numeric_precision = 5,
            time_zone = Sys.getenv('TZ'))
test_times_check = dbGetQuery(con = con, statement='select * from test_times')

stopifnot(all.equal(target=test_times, test_times_check))
dbExecute(con, 'DROP TABLE test_times;')

ex_trip = dbGetQuery(con, 'select * from ex_trip') 
setDT(ex_trip)

dbDisconnect(con)

# Remove columns from previous mode_type imputation
mt_names = c(str_subset(names(ex_trip), 'mode_type_'),
             'reported_mode_probability', 'predicted_mode_probability',
             'mode_imputation_type')

setnames(ex_trip, mt_names, paste0(mt_names, '_del'))



# Prediction dataset ----------------------------------------------------------

message('Creating dataset for mode_type prediction')

# Load input dataset
dt_base = readRDS(base_data_path)

dt_predict = create_input_dataset(dt_base, county_fips_in_region, predict=TRUE)

missing_no_response = -9998L
missing_other = 997L        

# mode_type_predicted =========================================================


# Get predictions

if (!'av_walk' %in% names(dt_predict) ){
  # availability
  dt_predict[, av_walk := 1]
  dt_predict[, av_bike := 1]
  dt_predict[, av_car := 1]
  dt_predict[, av_transit := 1]
  dt_predict[, av_tnc := 1]
}

database = dt_predict

apollo_beta = model$estimate
apollo_control = model$apollo_control
apollo_inputs = apollo_validateInputs()

pred_obj = apollo_probabilities(apollo_beta, apollo_inputs, functionality='prediction')
probs =as.data.table(pred_obj$model)
setnames(probs, names(probs), paste0('p_', names(probs)))

stopifnot(nrow(database) == nrow(probs))
probs$trip_id = database[, trip_id]
setcolorder(probs, c('trip_id', 'p_chosen'))

prob_check = merge(probs, 
                   dt_predict[, .(trip_id, choice_1, choice_2, choice_3, choice_4, choice_5)],
                   by = 'trip_id')


# Check the data --------------------------------------------------------------

# # All rows add to 1
prob_sums = probs[, (round(p_walk + p_bike + p_car + p_transit + p_tnc, 3))]
stopifnot(all(prob_sums[!is.na(prob_sums)] == 1))

# Model predicts reclassified mode types.  Here is the crosswalk
# (1) walk (mode_type=1),  
# (2) bike (mode_type=2 and mode_1, mode_2 or mode_3 = 2, 3 or 4) - does not include bike-share, scooter-share, moped-share
# (3) car (mode_type=3)
# (4) transit (mode_type=5)
# (5) TNC (mode_type=9)
# (6) other (all other cases)

mode_types = c(1, 2, 3, 5, 9)
mt_tbl = data.table(name = c('walk', 'bike', 'car', 'transit', 'tnc'),
                    mode_type = mode_types)
mt_tbl

names(probs)[-c(1, 2)] = str_c('mode_type_p_', mode_types)

# Get predicted mode type as mode_type with highest probability
# Keep the probability of that column

probs[, `:=`(mode_type_predicted = mode_types[which.max(.SD)],
             predicted_mode_probability = max(.SD)),
      .SDcols = str_c('mode_type_p_', mode_types), by = .(trip_id)]

probs[, .N, mode_type_predicted][order(mode_type_predicted)]
range(probs[, predicted_mode_probability], na.rm=TRUE)
range(probs[, p_chosen], na.rm=TRUE)

no_pred_ids = setdiff(ex_trip$trip_id, probs$trip_id)
length(no_pred_ids)

trip = merge(ex_trip, probs, by = 'trip_id', all.x=TRUE)

stopifnot(nrow(trip) == nrow(ex_trip))

# trip[is.na(mode_type_predicted), mode_type_predicted := -9998]

# Shares of the actual mode_types compared with predicted
pred = trip[, .N, mode_type_predicted
     ][order(mode_type_predicted), .(mode_type = mode_type_predicted, pred = round(N / sum(N), 2))]

obs = trip[, .N, mode_type
     ][order(mode_type), .(mode_type, obs = round(N / sum(N), 2))]

obs[pred, on = 'mode_type'] %>% knitr::kable(format='markdown')

with(trip, table(mode_type, mode_type_predicted, useNA='always')) %>%
  knitr::kable(format='pipe')


# Calculate mode_type_imputed ===============================================

# Apply predicted mode_type to the correct subset of rows, as specified in 
# Mark's e-mail of 1/09/2020.
# See https://teams.microsoft.com/l/message/19:dc20e6b23bec45afa016b3313af76d34@thread.skype/1578664582231?tenantId=93676b1f-9039-4fea-97db-dde5da5b29fa&groupId=cb5a7e46-3d5f-4ac6-b2d6-4f70ca6b3268&parentMessageId=1578664582231&teamName=Transportation%20MR&channelName=SB1_Data_Quality&createdTime=1578664582231


# Define region
state_name = 'California'
wgs84 = "+proj=longlat +ellps=WGS84"
planar_proj = '+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs'
# https://spatialreference.org/ref/epsg/2163/

ca = counties(state_name)
county_bounds = ca[ca$COUNTYFP %in% county_fips_in_region, ] %>%
  st_transform(planar_proj)

# Turn destination coordinates into spatial data frame
d_locs = st_as_sf(trip[, .(trip_id, d_lon, d_lat)],
                  coords=c('d_lon', 'd_lat'),
                  crs = wgs84) %>%
  st_transform(crs=planar_proj)
o_locs = st_as_sf(trip[, .(trip_id, o_lon, o_lat)],
                  coords=c('o_lon', 'o_lat'),
                  crs=wgs84) %>%
  st_transform(crs=planar_proj)

d_in_county = st_join(county_bounds, d_locs)
o_in_county = st_join(county_bounds, o_locs)
# data.table(d_in_county)[, .N, COUNTYFP]
# data.table(o_in_county)[, .N, COUNTYFP]

trip[, in_region := 0]
trip[trip_id %in% d_in_county$trip_id & trip_id %in% o_in_county$trip_id,
     in_region := 1]
trip[, .N, in_region]

mt_tnc = mt_tbl[name == 'tnc', mode_type]
mt_bike = mt_tbl[name == 'bike', mode_type]
mt_transit = mt_tbl[name == 'transit', mode_type]
mt_walk = mt_tbl[name == 'walk', mode_type]
mt_car = mt_tbl[name == 'car', mode_type]

trip[, `:=`(n_trips = .N,
            n_complete = sum(survey_complete_trip),
            n_incomplete = sum(survey_complete_trip == 0)),
     by = .(person_id, day_num)]

# -	Apply the model for all person-days that have at least one complete trip and
#   no more than 5 incomplete trips. 
# Except when:
# Don't apply the model when
#   1. at least one complete trip and
#   no more than 5 incomplete trips.
#   1. Trips where the reported mode does not correspond to one of the five modes
#      used in the model, or -9998 or "Other" (7)
#   2. Trips that go outside the region.
#   3. Trips that were user-added.
# Mode_type_imputed is reported mode_type for these three cases

#   -  If the reported mode was missing (incomplete) trip, set the imputed mode
#      to the highest probability mode from the model.
#   -  Otherwise, if the reported mode is TNC or the highest probability mode
#      from the model is TNC, then set the imputed mode to the reported mode
#      (We decided not to change any trips to or from TNC.)
#   -  Otherwise, if the reported mode is bike or transit, the probability for
#      the reported mode is less than 0.02 and the probability for the highest
#      probability mode is higher than 0.98, set the imputed mode to the highest
#      probability mode from the model.
#   -  Otherwise, if the reported mode is walk or car, the probability for the
#      reported mode is less than 0.15 and the probability for the highest
#      probability mode is higher than 0.75, set the imputed mode to the highest
#      probability mode from the model.
#   -  Otherwise, set the imputed mode to the reported mode.


# Treat mode_1 == 997 the same as mode_type == -9998

trip[mode_1 == 997, mode_type := missing_no_response]

# Get the index for the three exceptions
not_applied = trip[, n_complete < mincompletetrips |
                     n_incomplete > maxincompletetrips |
                     !mode_type %in% c(mode_types, missing_no_response) |
                     in_region == 0]

with(trip, table(mode_type, mode_type_predicted, useNA='always'))

trip[, mode_type_imputed := case_when(
  
  n_complete < mincompletetrips | n_incomplete > maxincompletetrips | !(mode_type %in% c(mode_types, missing_no_response)) |
#    added_trip |
    in_region == 0 ~ mode_type,

  mode_type %in% c(missing_no_response) ~ as.integer(mode_type_predicted),

  (mode_type == mt_tnc | mode_type_predicted == mt_tnc) ~ mode_type,

  ((mode_type == mt_bike & mode_type_p_2 < 0.02 & predicted_mode_probability > 0.98) |
     (mode_type == mt_transit & mode_type_p_5 < 0.02 & predicted_mode_probability > 0.98)) ~ as.integer(mode_type_predicted),

  ((mode_type == mt_walk & mode_type_p_1 < 0.15 & predicted_mode_probability > 0.75) |
     (mode_type == mt_car & mode_type_p_3 < 0.15 & predicted_mode_probability > 0.75)) ~ as.integer(mode_type_predicted),

  TRUE ~ mode_type
)]


# trip[!not_applied, .N, .(mode_type, mode_type_imputed)][order(mode_type, mode_type_imputed)]
with(trip[!not_applied], table(mode_type, mode_type_imputed, useNA='always')) %>%
  knitr::kable(format='pipe')

with(trip[mode_1 == 997], table(mode_type, mode_type_imputed, useNA='always'))

# All of the mode_type_imputed that are NA fail to meet the completion criteria
# trip[is.na(mode_type_imputed), .N, .(n_complete == 0, n_incomplete > 5)]

# trip[!(n_complete > 0 & n_incomplete <= 5), .N, mode_type_imputed][order(mode_type_imputed)]
# trip[not_applied, .N, mode_type_imputed]

# Assign -1 to all mode_type_imputed that have no value yet.
trip[is.na(mode_type_imputed), mode_type_imputed := -1]
trip[mode_type_imputed == -1, .N]
trip[is.na(mode_type_imputed), .N]

trip[, .N, .(mode_type, mode_type_imputed)
][order(mode_type, mode_type_imputed)]

trip[mode_type == missing_no_response, .N,
     .(mode_type, mode_type_predicted)][order(mode_type_predicted)]


# Add additional variables requested by Mark----------------------------------

# The extra variables that need to be written to the trip file are:
# +	reported_mode_probability: The model probability for the reported mode. 
#   (Set it to -1 if the reported mode has missing data or if it one of exceptions
#   1-3 above where the model isn’t applied)

trip[!mode_type %in% mode_types | added_trip == 1 | in_region == 0,
      p_chosen := -1]
setnames(trip, 'p_chosen', 'reported_mode_probability')
# stopifnot(trip[is.na(p_chosen), .N] == 0)

# + mode_type_predicted: The mode with the highest probability from the model,
#   recoded back to use the same codes as for mode_type (1,2,3,7,9)   
#   (Set it to -1 for the cases where the model isn’t applied).

trip[not_applied, mode_type_predicted := -1]

trip[not_applied, .N, mode_type_predicted][order(mode_type_predicted)]
trip[!not_applied, .N, mode_type_predicted][order(mode_type_predicted)]

# + predicted_mode_probability: The model probability for the predicted mode.
#   (Set it to -1 if it is one of exceptions 1-3 above where the model isn’t applied)

trip[mode_type_predicted == -1, predicted_mode_probability := -1]

# + mode_type_imputed: Set according to the rules above, with any model-based modes
#   recoded back to use the same codes as for mode_type.  
#   (Use -1 if there are any cases that do not have an imputed mode – e.g. an 
#   incomplete trip that is outside the region – but there should be very few such
#   cases). 

trip[, .N, mode_type_imputed][order(mode_type_imputed)]

# + mode_imputation_type: I would use values
#   -1= imputation model not used,
#   0=imputation model used and imputed mode=reported mode (no change);
#   1 = imputation model used and imputed mode not equal to reported mode
#       (changed to predicted mode),
#   2=imputation model used and missing value changed to predicted mode.

trip[, mode_imputation_type := case_when(
  not_applied ~ -1L,
  mode_type_imputed == mode_type ~  0L,
  mode_type != missing_no_response & mode_type_imputed != mode_type ~  1L,
  mode_type == missing_no_response & mode_type_imputed > -1 ~ 2L
)]

trip[, .N, mode_imputation_type]
trip[mode_type == missing_no_response, .N, .(mode_type, mode_type_predicted, mode_imputation_type)]
trip[is.na(mode_imputation_type), .N, mode_type]

# with(trip, table(mode_type_imputed, mode_type, mode_imputation_type))

trip[, .N, .(mode_type_imputed_del, mode_type_imputed)]
with(trip, table(mode_type_imputed_del, mode_type_imputed, useNA='always')) %>%
  knitr::kable(format='pipe')
trip[, mode_type_imputed := as.integer(mode_type_imputed)]

trip %>%
  tabyl(mode_type_imputed_del, mode_type_imputed) %>%
  knitr::kable(format='pipe')

trip %>%
  tabyl(mode_type_predicted_del, mode_type_predicted) %>%
  knitr::kable(format='pipe')

# Write a temporary version for Mark
output_path = file.path(model_dir, paste0('trip_', dbname, '_', model_version, '.csv'))
fwrite(trip, output_path)
zip(str_replace(output_path, '.csv', '.zip'), files=output_path)

# # Write data ------------------------------------------------------------------
# 
# message('Checking data for errors before writing')
# keep_cols = c(
#   names(ex_trip),
#   str_c('mode_type_p_', mode_types),
#   'mode_type_predicted', 'mode_type_imputed',
#   'reported_mode_probability', 'predicted_mode_probability',
#   'mode_imputation_type')
# 
# trip = trip[, keep_cols, with=FALSE]
# 
# # Check differences in names
# (not_in_new = setdiff(names(ex_trip), names(trip)))
# (not_in_old = setdiff(names(trip), names(ex_trip)))
# 
# 
# # Make sure we haven't changed the number of rows
# 
# stopifnot((nrow(ex_trip) - nrow(trip) == 0))
# 
# if (write_results_to_pops){
#   
#   message('Writing table to POPS')
#   
#   con = connect_to_pops(dbname)
#   
#   write_to_db(con, trip, 'ex_trip', overwrite = TRUE,
#               numeric_scale = 13, numeric_precision = 5,
#               timezone = Sys.getenv('TZ'))
#   
#   
#   message('Success! Now checking database copy for errors')
#   
#   dt_test = read_from_db(con, "select * from ex_trip")
#   
#   DBI::dbDisconnect(con)
#   
#   setorder(dt_test, trip_id)
#   setorder(trip, trip_id)
#   
#   dt_test[trip, on = 'trip_id'][mode_type_p_1 != i.mode_type_p_1, .(mode_type_p_1, i.mode_type_p_1)]
#   
#   stopifnot(
#     all.equal(dt_test, trip, tolerance=0.001, check.attributes=FALSE),
#     sapply(dt_test, class)$hh_id == 'numeric',
#     sapply(trip, class)$hh_id == 'numeric',
#     nrow(trip) == nrow(dt_test),
#     length(setdiff(dt_test[, trip_id],
#                    trip[, trip_id])) == 0,
#     all(dt_test[trip, on = .(trip_id)
#     ][, hh_id == i.hh_id]))
#   message('Tests passed!')
#   
# }



