


# Load libraries ==============================================================
library(DBI)
# library(tmrtools)

# Use dev version of tmrtools
tmrtools_dir = 'C:/Users/matt.landis/OneDrive - Resource Systems Group, Inc/Git/tmrtools'
devtools::load_all(tmrtools_dir)
library(data.table)
library(stringr)
library(apollo)

# Utility function ============================================================

use_prev_params = function(cur_params, prev_params){
  
  Cur = data.table(name = names(cur_params), cur=cur_params)
  Cur[, sort := 1:.N]
  
  Prev = data.table(name = names(prev_params), prev=prev_params)
  
  dt = merge(Cur, Prev, by='name', all.x=TRUE)
  setorder(dt, sort)
  
  dt[, param := fifelse(is.na(prev) & !is.na(cur), cur, prev)]

  params = setNames(dt$param, dt$name)
  
  
  stopifnot(all.equal(names(cur_params), names(params)))
  
  return(params)
}

# Set parameters ===============================================================

# setwd('TaskOrder8/8.4_mode_imputation')

args = commandArgs()

file_arg = args[str_detect(args, '--file')]
file_arg = str_replace(file_arg, '--file=', '')
base_dir = dirname(file_arg)

arg_idx = which(str_detect(args, '--args'))

if ( length(arg_idx) > 0){
  
  dbname = args[arg_idx + 1]
  model_version = args[arg_idx + 2]
  
} else {
  dbname = 'tnc_bayarea'
  model_version = 'v2.1'
}

model_dir = file.path(dbname, model_version)

# Make sure we're in the right directory
if ( !dir.exists(model_dir) ){
  setwd('TaskOrder8/8.4_mode_imputation')
}
stopifnot(dir.exists(model_dir))

message('\n------------\ndbname is ', dbname, '; model_version is ', model_version)

input_functions_path = paste0(model_version, '_input_functions.R')

base_data_path = file.path(model_dir, 'base_data.rds')
estimation_data_path = file.path(model_dir, 'estimation_data.rds')
model_path = file.path(model_dir, 'model.rds')

source(input_functions_path) # Contains apollo_probabilities and apollo_fixed

# Create base data set ===================================================

# FIXME: Load from flat files

if (!file.exists(base_data_path)){
  con = connect_to_pops(dbname)

  message('Reading ex_trip')  
  ex_trip = dbGetQuery(con, 'select * from ex_trip') 
  setDT(ex_trip)
  
  message('Reading ex_location')
  ex_location = dbGetQuery(con, 'select * from ex_location')
  setDT(ex_location)
  
  message('Reading ex_hh')
  ex_hh = dbGetQuery(con, 'select * from ex_hh')
  setDT(ex_hh)
  
  message('Reading ex_person')
  ex_person = dbGetQuery(con, 'select * from ex_person')
  setDT(ex_person)
  
  dbDisconnect(con)
  
  dt_base = create_base_dataset(
    hh=ex_hh,
    person=ex_person,
    trip=ex_trip,
    location=ex_location)
  saveRDS(dt_base, base_data_path)
  
} else {
  
  dt_base = readRDS(base_data_path)

}


# Create "input" dataset =======================================================
# aka estimation dataset

n_na = dt_base[is.na(travel_date_dow), .N]

if ( n_na > 0 ){
  warning('Dropped ', n_na, ' missing values for travel_date_dow')
  
  dt_base = dt_base[!is.na(travel_date_dow)]
}

county_fips_in_region = switch(dbname,
                               tnc_bayarea = c("075", "013", "085", "001", "081", "097", "041", "095", "055"),
                               tnc_sandag = c('073'),
                               tnc_scag = c('037', '059'))

dt_input = create_input_dataset(dt_base, county_fips_in_region, predict=FALSE)

saveRDS(dt_input, estimation_data_path)


# Estimate model ===============================================================

### Initialise code
apollo_initialise()

### Set core controls
apollo_control = list(
  modelName  = "MNL",
  modelDescr = "MNL model",
  indivID    = "ID"
)

# ################################################################# #
#### LOAD DATA AND APPLY ANY TRANSFORMATIONS                     ####
# ################################################################# #

database = dt_input

# ################################################################# #
#### DEFINE MODEL PARAMETERS                                     ####
# ################################################################# #
apollo_beta_null = setNames(rep(0, length(param_names)), param_names)

# FIXME: Update to use apollo function

# prev_model_directory = '../../Data Cleaning and Processing/data_processing_2019_spring/Export Views/mode_imputation_dev'
# prev_model_file = switch(dbname,
#                          tnc_bayarea = 'mode_imputation_model_bayarea_13.rds',
#                          tnc_sandag = 'mode_imputation_model_sandag_3.rds',
#                          tnc_scag = 'mode_imputation_model_scag.rds')
# prev_model_path = file.path(prev_model_directory, prev_model_file)

prev_model_path = file.path(dbname, 'v1', 'model.rds')

if ( exists('prev_model_path') ){
  
  message('Previous model: ', prev_model_path)
  
  prev_model = readRDS(prev_model_path)
  prev_params = prev_model$estimate
  stopifnot(!is.null(prev_params))
  
  apollo_beta = use_prev_params(cur_params=apollo_beta_null, prev_params=prev_params)
  
  # Set fixed parameters back to zero
  
  cat('\nFixing the following betas at zero:\n\t', paste(apollo_fixed, collapse='\n\t'), '\n\n')
  
  stopifnot(all(apollo_fixed %in% names(apollo_beta)),
            all(apollo_beta[apollo_fixed] == 0))
  
  apollo_beta[apollo_fixed] = 0

} else {
  message('No previous model specified. Using null starts')
  apollo_beta = apollo_beta_null
}


# ################################################################# #
#### GROUP AND VALIDATE INPUTS                                   ####
# ################################################################# #

apollo_inputs = apollo_validateInputs()


# ################################################################# #
#### MODEL ESTIMATION                                            ####
# ################################################################# #

model =
  apollo_estimate(
    apollo_beta,
    apollo_fixed,
    apollo_probabilities,
    apollo_inputs,
    estimate_settings=list(writeIter=FALSE))

saveRDS(model, model_path)

# ################################################################# #
#### MODEL OUTPUTS                                               ####
# ################################################################# #

# ----------------------------------------------------------------- #
#---- FORMATTED OUTPUT (TO SCREEN)                               ----
# ----------------------------------------------------------------- #

apollo_modelOutput(model)


# End