# detailed modes by access/egress 
library(data.table)
library(rprojroot)
library(tmrtools)

# region = 'bayarea'
region = 'sandag'
# region = 'scag'

# set wds and get data
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
data_dir = file.path(sp_dir, 'Data')

work_dir = file.path(data_dir, 'Working_data')

trip_linked = readRDS(file.path(work_dir, paste0('trips_linked_', region, '.rds')))

# label data


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
varvals = copy(codebook)

variable_labels = unique(varvals[,  .(variable, description = variable, logic = '')])
variable_labels[, variable_order := seq_len(.N)]

variable_labels = variable_labels[variable %in% names(trip_linked)]

varvals= varvals[!is.na(value_label)]
varvals= varvals[!is.na(value)]

trip_linked_labeled = factorize_df(trip_linked, vals_df = varvals)


# make tables
trip_linked_labeled[is_transit == 1  & as.numeric(mode_type) == 5 & !as.numeric(mode_priority) %in% c(995, -9998), .N, mode_priority]

access_tab = cross_tab(trip_linked[is_transit == 1 & mode_type == 5 & !mode_priority %in% c(995, -9998)], 
                       variable_labels = variable_labels, value_labels = varvals, variables = c('mode_priority'),
                       tab_by = c('access_mode_type'), rounding_precision = 0, row_totals = TRUE )

egress_tab = cross_tab(trip_linked[is_transit == 1 & mode_type == 5 & !mode_priority %in% c(995, -9998)], 
                       variable_labels = variable_labels, value_labels = varvals, variables = c('mode_priority'), 
                       tab_by = c('egress_mode_type'), rounding_precision = 0, row_totals = TRUE )

access_tab[, value_label := gsub('[0-9]', '', value_label)]
names(access_tab) = gsub('[0-9]', '', names(access_tab))

egress_tab[, value_label := gsub('[0-9]', '', value_label)]
names(egress_tab) = gsub('[0-9]', '', names(egress_tab))
# copy to excel and format

switch(region,
       bayarea = write.table(access_tab[order(-Total)][c(3:nrow(access_tab), 2), 
                                                       c(5, 19, 8, 14, 13, 10, 18, 15, 9, 11, 17, 12, 16, 7) ], 
                             'clipboard', sep = '\t', row.names = FALSE),
       sandag = write.table(access_tab[order(-Total)][c(3:nrow(access_tab), 2), 
                                                      c(5, 16, 8, 12, 11, 9, 15, 10, 14, 13, 7) ], 
                            'clipboard', sep = '\t', row.names = FALSE),
       scag = 'label_scag')


switch(region,
       bayarea = write.table(egress_tab[order(-Total)][c(3:nrow(access_tab), 2), 
                                                       c(5, 19, 8, 14, 13, 10, 18, 15, 9, 11, 17, 12, 16, 7) ], 
                             'clipboard', sep = '\t', row.names = FALSE),
       sandag = 'label_sandag',
       scag = 'label_scag')

access_tab[order(-Total)][c(3:nrow(access_tab), 2), 
                          c(5, 16, 8, 12, 11, 9, 15, 10, 14, 13, 7) ]
