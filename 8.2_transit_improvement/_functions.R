# Functions for transit trip relinking and access/egress insertion
# 2 Feb 2021 
# RSG, Inc.
necessary_packages = c("readxl")

new_packages = setdiff(necessary_packages, installed.packages()[,"Package"])

if(length(new_packages) > 0) {
  install.packages(new_packages)
}

#' Read a codebook.
#'
#' Reads in an Excel format codebook and returns a \code{data.table} of variables and
#' values.
#'
#' Currently reads two formats of codebook.  One format ("format 1") has variables and values
#' for all tables in a single sheet, typically called \code{Values}.  The other format
#' ("format 2") has each database table in its own worksheet, with sheets called \code{hh},
#' \code{person}, \code{day}, \code{vehicle}, \code{trip} and \code{location}.
#' If \code{varvals} is TRUE and \code{sheet} is not found in the Excel workbook,
#' the function will load separate worksheets per database table using the
#' names listed above. Names not found in the worksheets will be ignored.
#'
#' @param codebook_path Character string. The full path to the codebook
#' @param varvals Logical. Whether to return the variables and values,
#'  or the variable overview (variables and descriptions).
#' @param sheet Character string.  The name of the sheet to read.  Defaults to
#'  "Values" if \code{varvals} is TRUE and "Overview" if FALSE.
#' @param label_col Character string.  The name of the column to use for value
#'   labels.  Defaults to "label".
#' @return Returns a \code{data.table} with the categorical variables and their values
#'  (if \code{varvals}) and otherwise a \code{data.table} with all variables
#'  and their descriptions.
#' @export
#'
read_codebook =
  function(
    codebook_path,
    varvals = TRUE,
    sheet = ifelse(varvals, 'Values', 'Overview'),
    label_col = 'label'){
    require('readxl')
    
    if (varvals) {
      
      sheet_names = readxl::excel_sheets(codebook_path)
      
      if (sheet %in% sheet_names) {
        vvalues = readxl::read_excel(path = codebook_path, sheet = sheet)
        setDT(vvalues)
        
      } else {
        
        # multi-sheet format codebook
        sheets = c('hh', 'person', 'day', 'vehicle', 'trip', 'location')
        vvalue_list = lapply(sheets, function(x){
          if (x %in% sheet_names) {
            message('Reading codebook sheet ', x)
            readxl::read_xlsx(codebook_path, sheet = x)
          } else {
            NULL
          }
        })
        
        vvalues = rbindlist(vvalue_list)
        vvalues = unique(vvalues)
      }
      
      vvalues[, label_value := paste(value, label_col)]
      
      if (!'val_order' %in% names(vvalues)) {
        vvalues[, value := as.numeric(value)]
        setorder(vvalues, variable, value)
        vvalues[, val_order := 1:.N, by = .(variable)]
      }
      
      # Add missing values -9998 and 995
      
      # Update using data.table::CJ
      # # edit vvalues to contain values for -9998, 995 for all variables that already have labels.
      # vvalues_supplement = crossing(
      #   variable = unique(vvalues$variable),
      #   value = c("-9998", "995")) %>%
      #   left_join(
      #     data.frame(
      #       value = c("-9998", "995"),
      #       label = c("-9998 Missing: Non-response", "995 Missing: Skip logic"),
      #       label_text_only = c("Missing: Non-response","Missing: Skip logic"),
      #       val_logic = NA,
      #       val_order = c(100000, 100001),
      #       stringsAsFactors = FALSE),
      #     by = c("value")
      #   )
      #
      # vvalues = rbind(vvalues, vvalues_supplement)
      
      return(vvalues[])
      
    } else {
      # read in variable labels and logic
      varnames = read_excel(path = codebook_path, sheet = sheet)
      setDT(varnames)
      return(varnames[])
    }
  }

#' get_prev
#'
#' Lagging
#'
#' @param overlap_var Variable to lag
#' @param ... list of parameters to pass to shift
get_prev =
  function(
    overlap_var,
    ...) {
    
    return(shift(overlap_var, type = 'lag', ...))
  }


#' get_next
#'
#' Leading
#'
#' @param overlap_var Variable to lead
#' @param ... list of parameters to pass to shift
get_next =
  function(
    overlap_var,
    ...) {
    
    return(shift(overlap_var, type = 'lead', ...))
  }
