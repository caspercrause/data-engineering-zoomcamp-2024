suppressPackageStartupMessages(expr = {
    library(googlesheets4)
    library(tidyverse)
    library(curl)
    library(readr)
})

Sys_time          <- format(Sys.time(), "%A, %b %d, %Y at %H:%M:%S") 
mymsg             <- sprintf("Executedon `%s`", Sys_time)

print(
 mymsg
)