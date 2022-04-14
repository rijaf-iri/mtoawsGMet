#' Update hourly spatial data database.
#'
#' Update hourly spatial data from AWS to ADT database.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.\cr
#' 
#' @export

update_dataHour_sp <- function(aws_dir){
    netInfo <- aws_network_info()
    nb_net <- netInfo$nbnet

    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "LOGPROC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "update_aws_data.txt")

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        msg <- "Unable to connect to ADT server\n"
        format.out.msg(msg, logPROC)

        return(1)
    }

    for(n in 1:nb_net){
        ret <- try(update.aws_data(conn, aws_dir, n), silent = TRUE)
        if(inherits(ret, "try-error")){ 
            mserr <- gsub('[\r\n]', '', ret[1])
            msg <- paste(ret, "Updating aws_data table failed")
            format.out.msg(paste(mserr, '\n', msg), logPROC)
        }
    }

    DBI::dbDisconnect(conn)

    return(0)
}

update.aws_data <- function(conn, dirAWS, network){
    netInfo <- aws_network_info()
    netCRDS <- netInfo$coords
    netPARS <- netInfo$pars

    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    crds <- DBI::dbReadTable(conn, netCRDS[network])
    lastTb <- DBI::dbReadTable(conn, "aws_aggr_period")

    for(i in seq_along(crds$id)){
        last <- lastTb$hour_sp_end[lastTb$network == network & lastTb$id == crds$id[i]]
        if(last == 0){
            last <- as.POSIXct("2015-01-01 00:00:00", tz = tz)
        }else{
            last <- as.POSIXct(as.integer(last), origin = origin, tz = tz) - 60 * 60
        }
        last <- as.integer(last)
        query <- paste0("SELECT * FROM aws_data0 WHERE (network=", network, " AND id='", crds$id[i], "') AND obs_time > ", last)
        qres <- DBI::dbGetQuery(conn, query)
        if(nrow(qres) == 0) next

        qres$value[!is.na(qres$limit_check)] <- NA

        id_obs <- qres[, c('height', 'var_code')]
        id_obs <- id_obs[!duplicated(id_obs, fromLast = TRUE), , drop = FALSE]

        ## sep wind hrer
        # iw <- id_obs$var_code %in% 9:12
        # id_wind <- id_obs[iw, , drop = FALSE]
        # id_obs <- id_obs[!iw, , drop = FALSE]

        dat_1hr <- lapply(seq(nrow(id_obs)), function(j){
            obs_hgt <- id_obs$height[j]
            obs_var <- id_obs$var_code[j]
            ix <- qres$height == obs_hgt & qres$var_code == obs_var
            qvar <- qres[ix, , drop = FALSE]

            dat_var <- getAWSObs_1hr_SP(qvar)
            return(dat_var)
        })

        dat_1hr <- do.call(rbind, dat_1hr)
        name_dat <- names(dat_1hr)

        ### limit check
        varTable <- DBI::dbReadTable(conn, netPARS[network])
        var_nm <- c("var_height", "var_code", "stat_code")
        id_tab <- do.call(paste, c(varTable[var_nm], sep = "-"))
        var_nm <- c("height", "var_code", "stat_code")
        id_dat <- do.call(paste, c(dat_1hr[var_nm], sep = "-"))
        ix <- match(id_dat, id_tab)
        varTable <- varTable[ix, c("min_val", "max_val")]
        varTable$min_val[is.na(varTable$min_val)] <- -Inf
        varTable$max_val[is.na(varTable$max_val)] <- Inf

        dat_1hr$limit_check[dat_1hr$value < varTable$min_val] <- 1
        dat_1hr$limit_check[dat_1hr$value > varTable$max_val] <- 2

        ###
        fun_format <- list(as.integer, as.character, as.numeric,
                           as.integer, as.integer, as.integer,
                           as.numeric, as.integer)
        dat_1hr <- formatTablesColumns(dat_1hr, fun_format, name_dat)
        dat_1hr$obs_id <- getObsId(dat_1hr)

        deleteDuplicatedObs(conn, "aws_data", dat_1hr$obs_id)
        DBI::dbWriteTable(conn, "aws_data", dat_1hr, append = TRUE, row.names = FALSE)

        last <- max(dat_1hr$obs_time, na.rm = TRUE)
        statement <- paste0("UPDATE aws_aggr_period SET hour_sp_end=", last,
                            " WHERE network=", network, " AND id='", crds$id[i], "'")
        DBI::dbExecute(conn, statement)
    }

    return(0)
}