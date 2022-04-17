#' Update daily data database.
#'
#' Update daily data from AWS to ADT database.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' @param dailyRainfall logical, if \code{TRUE} update daily rainfall only, if \code{FALSE} update other parameters.
#' @param dailyRainObsHour observation hour for daily rainfall data.
#' 
#' @export

update_dataDay_db <- function(aws_dir, dailyRainfall = FALSE, dailyRainObsHour = 8){
   on.exit(DBI::dbDisconnect(conn))

    netInfo <- aws_network_info()
    nb_net <- netInfo$nbnet

    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "LOGPROC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "update_aws_daily.txt")

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        msg <- "Unable to connect to ADT server\n"
        format.out.msg(msg, logPROC)

        return(1)
    }

    mfracFile <- file.path(aws_dir, "AWS_DATA/JSON", "Min_Frac_Daily.json")
    minFrac <- jsonlite::read_json(mfracFile)

    for(n in 1:nb_net){
        ret <- try(populate.aws_daily(conn, aws_dir, n, minFrac, dailyRainfall, dailyRainObsHour), silent = TRUE)
        if(inherits(ret, "try-error")){ 
            mserr <- gsub('[\r\n]', '', ret[1])
            msg <- paste(ret, "Updating aws_daily table failed")
            format.out.msg(paste(mserr, '\n', msg), logPROC)
        }
    }

    return(0)
}

populate.aws_daily <- function(conn, dirAWS, network, minFrac, rrUpdate, rrObsHour){
    netInfo <- aws_network_info()
    netCRDS <- netInfo$coords

    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"
    var_out <- c("network", "id", "height", "obs_time",
                 "var_code", "stat_code", "value")

    crds <- DBI::dbReadTable(conn, netCRDS[network])
    lastTb <- DBI::dbReadTable(conn, "aws_aggr_period")

    if(rrUpdate){
        var_obs <- 5
    }else{
        var_obs <- as.integer(names(minFrac))
        var_obs <- var_obs[var_obs != 5]
    }

    for(i in seq_along(crds$id)){
        last <- lastTb$day_ts_end[lastTb$network == network & lastTb$id == crds$id[i]]
        if(last == 0){
            last <- as.Date("2015-01-01")
            dlast <- as.integer(last)
        }else{
            dlast <- last
            last <- as.Date(as.integer(last), origin = origin) - 1
        }

        if(rrUpdate){
            last <- as.integer(as.POSIXct(last, tz = tz) + rrObsHour * 60 * 60)
            query <- paste0("SELECT * FROM aws_hourly WHERE (network=", network,
                            " AND id='", crds$id[i], "' AND var_code=5)",
                            " AND obs_time > ", last)
        }else{
            last <- as.integer(as.POSIXct(last, tz = tz))
            query <- paste0("SELECT * FROM aws_hourly WHERE (network=", network,
                            " AND id='", crds$id[i], "') AND obs_time > ", last)
        }

        qres <- DBI::dbGetQuery(conn, query)
        if(nrow(qres) == 0) next

        id_obs <- which(qres$var_code %in% var_obs)
        if(length(id_obs) == 0) next

        qres <- qres[id_obs, , drop = FALSE]

        qres$value[!is.na(qres$spatial_check)] <- NA

        id_obs <- qres[, c('height', 'var_code')]
        id_obs <- id_obs[!duplicated(id_obs, fromLast = TRUE), , drop = FALSE]

        dat_day <- lapply(seq(nrow(id_obs)), function(j){
            obs_hgt <- id_obs$height[j]
            obs_var <- id_obs$var_code[j]
            ix <- qres$height == obs_hgt & qres$var_code == obs_var
            qvar <- qres[ix, , drop = FALSE]
            if(nrow(qvar) == 0) return(NULL)

            dat_var <- lapply(unique(qvar$stat_code), function(s){
                fun <- switch(as.character(s), "1" = mean, "2" = min,
                                        "3" = max, "4" = sum, mean)
                xval <- qvar[qvar$stat_code == s, , drop = FALSE]

                daty <- as.POSIXct(xval$obs_time, origin = origin, tz = tz)
                times <- format(daty, "%Y%m%d%H")

                if(rrUpdate){
                    index <- get_index_minhour2day_end(times, "hourly", rrObsHour, tz)
                }else{
                    index <- get_index_minhour2day_end(times, "hourly", 0, tz)
                }

                nobs <- sapply(index, length)
                avail_frac <- as.numeric(nobs)/24

                odaty <- as.Date(names(index), "%Y%m%d")

                xout <- xval[1, var_out, drop = FALSE]
                xout <- xout[rep(1, length(odaty)), , drop = FALSE]
                xout$obs_time <- as.numeric(odaty)
                xout$value <- NA
                ina <- avail_frac >= minFrac[[as.character(obs_var)]]

                xout$value[ina] <- sapply(index[ina], function(iv){
                    x <- xval$value[iv]
                    if(all(is.na(x))) return(NA)
                    fun(x, na.rm = TRUE)
                })

                xout$qc_output <- NA
                xout$miss_frac <- avail_frac

                xout
            })

            dat_var <- do.call(rbind, dat_var)
            dat_var <- dat_var[!is.na(dat_var$value), , drop = FALSE]

            dat_var
        })

        inull <- sapply(dat_day, is.null)
        if(all(inull)) next

        dat_day <- dat_day[!inull]
        dat_day <- do.call(rbind, dat_day)
        name_dat <- names(dat_day)
        if(nrow(dat_day) == 0) next

        dat_day <- dat_day[dat_day$obs_time >= dlast, , drop = FALSE]
        if(nrow(dat_day) == 0) next

        ### qc check (climatology)

        ##### 

        fun_format <- list(as.integer, as.character, as.numeric,
                           as.integer, as.integer, as.integer,
                           as.numeric, as.integer, as.numeric)
        dat_day <- formatTablesColumns(dat_day, fun_format, name_dat)
        dat_day$obs_id <- getObsId(dat_day)

        ######
        # update last record
        vlast <- dat_day$obs_time == dlast & dat_day$var_code %in% var_obs
        dat_last <- dat_day[vlast, , drop = FALSE]

        if(nrow(dat_last) > 0){
            if(rrUpdate){
                query <- paste0("SELECT * FROM aws_daily WHERE network=", network,
                                " AND id='", crds$id[i], "' AND var_code=5 AND obs_time=", dlast)
            }else{
                query <- paste0("SELECT * FROM aws_daily WHERE network=", network,
                                " AND id='", crds$id[i], "' AND obs_time=", dlast)
            }
            dat_db <- DBI::dbGetQuery(conn, query)

            if(nrow(dat_db) > 0){
                idup <- dat_last$obs_id %in% dat_db$obs_id
                if(any(idup)){
                    dat_last <- dat_last[idup, , drop = FALSE]
                    dat_last$qc_output[is.na(dat_last$qc_output)] <- 'NULL'
                    for(j in seq(nrow(dat_last))){
                        statement <- paste0("UPDATE aws_daily SET value=", dat_last$value[j],
                                            ", qc_output=", dat_last$qc_output[j],
                                            ", miss_frac=", dat_last$miss_frac[j],
                                            " WHERE network=", dat_last$network[j],
                                            " AND id='", dat_last$id[j],
                                            "' AND height=", dat_last$height[j],
                                            " AND obs_time=", dat_last$obs_time[j],
                                            " AND var_code=", dat_last$var_code[j],
                                            " AND stat_code=", dat_last$stat_code[j])
                        DBI::dbExecute(conn, statement)
                    }

                    dat_day <- dat_day[!dat_day$obs_id %in% dat_last$obs_id, , drop = FALSE]
                    if(nrow(dat_day) == 0) next
                }
            }
        }

        #####

        DBI::dbWriteTable(conn, "aws_daily", dat_day, append = TRUE, row.names = FALSE)

        last <- max(as.integer(dat_day$obs_time), na.rm = TRUE)
        statement <- paste0("UPDATE aws_aggr_period SET day_ts_end=", last,
                            " WHERE network=", network, " AND id='", crds$id[i], "'")
        DBI::dbExecute(conn, statement)
    }

    return(0)
}

