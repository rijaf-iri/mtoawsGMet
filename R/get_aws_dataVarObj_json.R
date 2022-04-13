#' Create AWS variables JSON file for minute, hourly and daily data.
#'
#' Create AWS variables JSON file for minute, hourly and daily data.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

get_aws_dataVarObj <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn))
        stop("Unable to connect to ADT server\n")

    netPARS <- c("adcon_synop_pars", "adcon_aws_pars", "tahmo_pars")

    varTable <- lapply(seq_along(netPARS), function(n){
        DBI::dbReadTable(conn, netPARS[n])
    })

    ## hourly & daily
    ihd <- c('var_code', 'var_name', 'var_unit', 'var_height')
    tab <- lapply(varTable,  function(x) x[, ihd])
    tab <- do.call(rbind, tab)
    tab <- tab[!duplicated(tab), ]
    names(tab) <- c('var_code', 'var_name', 'var_units', 'height')

    tab_day <- tab[tab$var_code %in% c(10, 2, 6, 1, 8, 5), ]
    tab_day <- list(variables = tab_day)

    path_day <- file.path(aws_dir, "AWS_DATA", "JSON", 'AWS_dataDayVarObj.json')
    unlink(path_day)
    jsonlite::write_json(tab_day, path_day, auto_unbox = TRUE, pretty = TRUE)

    tab_hour <- tab[tab$var_code %in% c(1, 2, 3, 5, 6, 7, 8, 10, 14), ]
    tab_hour <- list(variables = tab_hour)

    path_hour <- file.path(aws_dir, "AWS_DATA", "JSON", 'AWS_dataHourVarObj.json')
    unlink(path_hour)
    jsonlite::write_json(tab_hour, path_hour, auto_unbox = TRUE, pretty = TRUE)

    ## Minutes data
    ihd <- c('var_code', 'var_name', 'var_unit', 'var_height', 'stat_code', 'var_stat')
    tab <- lapply(varTable, function(x) x[, ihd])
    tab <- do.call(rbind, tab)
    tab <- tab[!duplicated(tab), ]
    names(tab) <- c('var_code', 'var_name', 'var_units', 'height', 'stat_code', 'stat_name')

    tab$var_units[is.na(tab$var_units)] <- ""
    tab <- list(variables = tab)

    path_min <- file.path(aws_dir, "AWS_DATA", "JSON", 'AWS_dataMinVarObj.json')
    unlink(path_min)
    jsonlite::write_json(tab, path_min, auto_unbox = TRUE, pretty = TRUE, na = "string")

    return(0)
}

