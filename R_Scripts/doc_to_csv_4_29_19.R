#!/usr/bin/env Rscript
library(stringi)
library(stringr)
library(textreadr)
library(R.utils)
library(purrr)

# Global Variables #
#######################################################################################
# log file path
log_path <<- "Y:\\Shared\\USD\\Business Data and Analytics\\Unstructured_Data_Pipeline\\DMS_Claims\\logging"

# get the current log file
today <- gsub("-","_",as.character(Sys.Date()))
log_filename <<- paste0(log_path, paste0("data_pipeline_log_", paste0(today, ".log")))


test_write_path <<- "Y:\\Shared\\USD\\Business Data and Analytics\\Unstructured_Data_Pipeline\\DMS_Claims\\doc_to_csv_test"
test_file_path <<- "V:\\Dev\\Historical\\20190521\\Document"
write_path <<- "Y:\\Shared\\USD\\Business Data and Analytics\\Unstructured_Data_Pipeline\\DMS_Claims\\doc_to_csv"


# doc to csv conversion functions #
#######################################################################################
# convert .doc file to .csv
to_csv <- function(file_name, timeout) {
  tryCatch({
    res <- R.utils::withTimeout({
      read_doc(file=file_name)
    }, timeout=timeout, substitute=T)
  }, TimeoutException=function(ex) {
    # add back file=log_filename to cat(paste0())
    cat(paste0("R Function(Timeout Error) File: ", file_name), file=log_filename, append=T, sep="\n")
    message("Timout Error. Skipping.")
  })
  
  return(res)
}

# runs the conversion of .doc to .csv
run_conversion <- function(file_path, write_path, timeout) {
  files <- list.files(path=file_path, pattern='.doc$')
  full_files <- paste(file_path, paste0("\\", files), sep="")
  for (i in 1:length(full_files)) {
    tryCatch({
      parsed_filename <- gsub(".doc", ".csv", files[i])
      content <- to_csv(full_files[i], timeout=timeout)
      
      write_name <- paste0(write_path, paste0("\\", parsed_filename))
      print(paste0("writing file, ", write_name))
      
      # write the file to the specified filename
      try(write.csv(content, file = write_name, row.names = F), silent=F)
      
    }, error=function(e){cat("ERROR:", conditionMessage(e), file=log_filename, append=T, sep="\n")})
  }
}


# main function # 
#######################################################################################
main <- function() {
  # Allow for the passing of command line arguments
  args <- commandArgs(trailingOnly = T)
  #output <- run_conversion(file_path = args[1], write_path = write_path, timeout = args[2])
  output <- run_conversion(file_path = args[1], write_path = write_path, timeout = 20)
}

# execute the main function
main()


