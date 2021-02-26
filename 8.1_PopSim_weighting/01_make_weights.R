# run_scripts.R

# 11 Feb 2021.
# RSG, Inc.

# Run any scripts necessary to perform and summarize output from
# PopulationSim weighting.


# setup =======================================================================

# Load libraries

library(stringr)
library(data.table)
library(lubridate)
library(zip)

# Directories 
# (Assumes working directory is location of this file)

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


# Directory to save zipfiles of output
zip_dir = file.path(work_dir, 'PopSim_results')
dir.create(zip_dir, showWarnings=FALSE)

# Specify directories

stage_dir = 'stage_data'

popsim_configs_dir = 'configs'
popsim_data_dir = 'data'
popsim_output_dir = 'output'

unlink(popsim_data_dir, recursive=TRUE)
dir.create(popsim_data_dir, showWarnings=FALSE)

unlink(popsim_output_dir, recursive=TRUE)
dir.create(popsim_output_dir, showWarnings=FALSE)


# Get path to python (in conda environment)
# 
cmd = 'conda info --envs'
conda_info = shell(cmd, intern=TRUE)
popsim_info = str_subset(conda_info, 'popsim')
stopifnot(length(popsim_info) == 1)

popsim_path = str_replace(popsim_info, 'popsim +', '')

message('Python path is: ', popsim_path)

# Load data

ss = fread(file.path(stage_dir, 'seed_stage.csv'))

whos = c('alladult', 'sphone')
days = c('wkday', '7day', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun')


# Work ========================================================================

# Loop over all the different weights.

for ( who in whos ){
  
  # Prepare control data ------------------------------------------------------
  
  # popsim is looking for control_totals_subregion.csv
  ctl = fread(file.path(stage_dir, str_glue('control_totals_subregion_{who}.csv')))
  ctl[, region := 1]
  ctl[, subregionDummy := subregion]
  
  # Make sure control targets are not NA
  puma_cols = str_subset(names(ctl), 'puma(s)?_')
  setnafill(ctl, fill=0, cols=puma_cols)
  
  # This is a person level study.  We assume each household is hh of one
  # ctl[, h_total := p_total]
  ctl[, h_total := NULL]
  setcolorder(ctl, c('region', 'subregion', 'subregionDummy'))

  # Get total number of people
  # h_total_ctl = ctl[, sum(h_total)] # NOTE: Not using household counts!
  p_total_ctl = ctl[, sum(p_total)]
  
  for ( day in days ){
    # day = days[1]
    
    # Create configs/control.csv and inputs/geo_cross_walk.csv
    if ( who == whos[1] & day == days[1] ){

      # Create configs/control file -------------------------------------------
      
      base_imp = 1000
      
      target_names = setdiff(names(ctl), c('region', 'subregion', 'subregionDummy', 'h_total'))
      
      cfg = data.table(target = target_names)
      cfg[, geography := 'subregion']
      cfg[, seed_table := 'households']
      cfg[, importance := fcase(
        target == 'p_total', base_imp * 4,
        default = base_imp)]
      cfg[, control_field := target]
      cfg[, expression := paste0(seed_table, '.', control_field, ' == 1')]
      cfg[target == 'p_total', expression := '(households.initial_wt > 0) & (households.initial_wt < np.inf)']
      
      fwrite(cfg, file.path(popsim_configs_dir, 'controls.csv'))
      
      
      # geo_cross_walk.csv ----------------------------------------------------------
      
      xwalk = data.table(
        region = 1,
        subregion = ctl[, subregion],
        subregionDummy = ctl[, subregion])
      
      fwrite(xwalk, file.path(popsim_data_dir, 'geo_cross_walk.csv'))
      
    } 
    
    
    zipfile = file.path(zip_dir, str_glue('popsim_{who}_{day}.zip'))
    # who = whos[1]
    message(str_glue('Getting weights for {who}, {day}'))
    
    if ( file.exists(zipfile) ){
      message('Zip file exists.  Skipping')
      next()
    }
    
    # Prepare seed files ------------------------------------------------------
    
    if ( day == '7day' ){
      
      filter_cond = 'n7daywts_complete == 7'
      
    } else if (day == 'wkday'){
      
      filter_cond = 'nwkdaywts_complete > 0'
      
    } else {
      # filter on specific weekday
      filter_cond = str_glue('{day}_complete == 1')
    }

    message('Filter condition is ', filter_cond)
    
    eval(parse(text = str_glue('seed = ss[{filter_cond}]')))

    stopifnot(seed[is.na(initial_wt), .N] == 0)
    
    message(str_glue('Filtered seed table from {ss[, .N]} rows to {seed[, .N]} rows'))
    
    # Adjust initial_wt by psp_prob
    if ( who == 'alladult' ){
      seed[, initial_wt := initial_wt / psp_prob]
    }
    
    # # Adjust sum of initial weights to match controls
    # seed[, initial_wt := initial_wt * (p_total_ctl / sum(initial_wt))]
    #
    # stopifnot(
    #  all.equal(seed_p[, sum(initial_wt)], p_total_ctl))
    
    # Check for missing values
    
    check_nas = seed[, lapply(.SD, function(x) sum(is.na(x)))] %>%
      melt(measure.vars=names(.))
    
    stopifnot(
      check_nas[value > 0, .N] == 0
    )
    
    # Create needed columns
    
    seed[, region := 1]

    # For some reason PopSim requires this
    seed[, subregionDummy := subregion]

    setcolorder(seed, c('region', 'subregion', 'subregionDummy'))
  
    
    # Split seed into seed_h and seed_p -------------------
    
    names_h = c(names(ctl), 'person_id', 'initial_wt')
    seed_h = seed[, names_h, with=FALSE]
    # seed_h[, h_total := 1]
    
    # hh_id is the same as person_id
    setnames(seed_h, old='person_id', new='hh_id')
    setcolorder(seed_h, c('region', 'subregion', 'subregionDummy', 'hh_id', 'initial_wt'))
    
    names_p = c('person_id', 'initial_wt')
    seed_p = seed[, names_p, with=FALSE]
    seed_p[, hh_id := person_id]
    setcolorder(seed_p, c('hh_id', 'person_id', 'initial_wt'))
    
    # Check names
    # Except for ids and weights, control file and seed files should have same columns
    ctl_names = names(ctl)
    seed_names = unique(c(names(seed_h), names(seed_p)))
    
    stopifnot(
      length(setdiff(ctl_names, seed_names)) == 0,
      all.equal(setdiff(seed_names, ctl_names), c('hh_id', 'initial_wt', 'person_id'))
    )
    
    
    # Save files

    fwrite(ctl, file.path(popsim_data_dir, 'control_totals_subregion.csv'))
    fwrite(seed_h, file.path(popsim_data_dir, 'seed_households.csv'))
    fwrite(seed_p, file.path(popsim_data_dir, 'seed_persons.csv'))
    
    # Run weights ----------------------------------------------------------------
    # Core function - run "run_populationsim.py"
    
    cmd = str_glue(
      '{popsim_path}\\python.exe run_populationsim.py --config {popsim_configs_dir} ',
      '--data {popsim_data_dir} --output {popsim_output_dir}')
    shell(cmd)
    
    
    # Rename output directory so it is not overwritten
    
    zip(zipfile=zipfile, files=c(popsim_configs_dir, popsim_data_dir, popsim_output_dir),
      recurse=TRUE, mode='mirror')
    
  }
}


# Collect output -------------------------------------------------------------

# Read files 

hh_dt = data.table()
for ( who in whos ){
  for ( day in days ){
    
    zipfile = file.path(zip_dir, str_glue('popsim_{who}_{day}.zip'))
    message('Reading ', basename(zipfile))
    
    tmp = tempfile()
    filename = 'final_summary_hh_weights.csv'
    unzip(
      zipfile,
      files=file.path('output', filename),
      junkpaths=TRUE,
      exdir=tmp)
    
    dt = fread(file.path(tmp, filename))
    colname = str_c('wt_', who, '_', day)
    
    setnames(dt, old='subregion_balanced_weight', new=colname)
    
    if ( nrow(hh_dt) == 0 ){
      hh_dt = copy(dt)
    } else {
      hh_dt = merge(hh_dt, dt, by = 'hh_id', all=TRUE)
    }
    
  }
}

# Get regions from seed_stage file

wt_cols = str_subset(names(hh_dt), 'wt')
setnafill(hh_dt, fill=0, cols=wt_cols)

# Change names in output to reduce confusion later
setnames(hh_dt, 'hh_id', 'person_id')

# Get additional columns from ss

hh_dt[, person_id := as.character(person_id)]
ss[, person_id := as.character(person_id)]

daynames = days[-c(1,2)]
ss_cols = c('person_id', 'region', 'subregion',
  str_c(daynames, '_status'), str_c(daynames, '_complete'),
  'nwkdaywts_complete', 'n7daywts_complete',
  'initial_wt', 'psp_prob')

person_dt = merge(hh_dt, ss[, ss_cols, with=FALSE], by = 'person_id')

stopifnot(person_dt[, .N] == hh_dt[, .N])

# Write out weights

outfile = file.path(work_dir, 'popsim_wt_person.csv')
fwrite(person_dt, outfile)


# Calculate day weights --------------------------------------------------------


setdiff(hh_dt[, person_id], ss[, person_id])
setdiff(ss[, person_id], hh_dt[, person_id])

hh_dt1 = merge(
  hh_dt,
  unique(ss[nwkdaywts_complete > 0, .(person_id, nwkdaywts_complete)]),
  by = 'person_id',
  all.x=TRUE
)

stopifnot(hh_dt[, .N] == hh_dt1[, .N, person_id][, .N])

mdt = melt(
  hh_dt1,
  id.vars = c(
    'person_id', 'nwkdaywts_complete',
    'wt_alladult_7day', 'wt_sphone_7day',
    'wt_alladult_wkday', 'wt_sphone_wkday'))

mdt[, who := str_extract(variable, '(alladult|sphone)')]
mdt[, dow := str_extract(variable, '[a-z]{3}$')]

mdt[, daywt_alladult_7day := wt_alladult_7day / 7]
mdt[, daywt_sphone_7day := wt_sphone_7day / 7]

mdt[, daywt_alladult_wkday := wt_alladult_wkday / nwkdaywts_complete]
mdt[, daywt_sphone_wkday := wt_sphone_wkday / nwkdaywts_complete]

dt = dcast(
  mdt,
  person_id + nwkdaywts_complete + daywt_alladult_7day + daywt_sphone_7day + 
    daywt_alladult_wkday + daywt_sphone_wkday + dow ~ who)

setnames(dt, old=c('alladult', 'sphone'), new=c('daywt_alladult_dow', 'daywt_sphone_dow'))

dt1 = merge(dt, ss[, .(person_id, region)], by = 'person_id')

stopifnot(dt1[, .N] == dt[, .N])

outfile = file.path(work_dir, 'popsim_wt_day.csv')
fwrite(dt1, outfile)
