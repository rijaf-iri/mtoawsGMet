#' Update hourly data database.
#'
#' Update hourly data from AWS to ADT database.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

update_dataHour_db <- function(aws_dir){
   on.exit(DBI::dbDisconnect(conn))

    netInfo <- aws_network_info()
    nb_net <- netInfo$nbnet

    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "LOGPROC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "update_aws_hourly.txt")

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        msg <- "Unable to connect to ADT server\n"
        format.out.msg(msg, logPROC)

        return(1)
    }

    mfracFile <- file.path(aws_dir, "AWS_DATA", "JSON", "Min_Frac_Hourly.json")
    minFrac <- jsonlite::read_json(mfracFile)

    for(n in 1:nb_net){
        ret <- try(update.aws_hourly(conn, aws_dir, n, minFrac), silent = TRUE)
        if(inherits(ret, "try-error")){ 
            mserr <- gsub('[\r\n]', '', ret[1])
            msg <- paste(ret, "Updating aws_hourly table failed")
            format.out.msg(paste(mserr, '\n', msg), logPROC)
        }
    }

    return(0)
}

update.aws_hourly <- function(conn, dirAWS, network, minFrac){
    netInfo <- aws_network_info()
    netCRDS <- netInfo$coords
    netPARS <- netInfo$pars
    tstep <- netInfo$tstep

    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    crds <- DBI::dbReadTable(conn, netCRDS[network])
    lastTb <- DBI::dbReadTable(conn, "aws_aggr_period")

    var_obs <- as.integer(names(minFrac))

    for(i in seq_along(crds$id)){
        last <- lastTb$hour_ts_end[lastTb$network == network & lastTb$id == crds$id[i]]
        if(last == 0){
            last <- as.POSIXct("2015-01-01 00:00:00", tz = tz)
            dlast <- as.integer(last)
        }else{
            dlast <- last
            last <- as.POSIXct(as.integer(last), origin = origin, tz = tz) - 60 * 60
        }
        last <- as.integer(last)

        query <- paste0("SELECT * FROM aws_data0 WHERE (network=", network, " AND id='", crds$id[i], "') AND obs_time > ", last)
        qres <- DBI::dbGetQuery(conn, query)
        if(nrow(qres) == 0) next

        id_obs <- which(qres$var_code %in% var_obs)
        if(length(id_obs) == 0) next

        qres <- qres[id_obs, , drop = FALSE]

        qres$value[!is.na(qres$limit_check)] <- NA

        id_obs <- qres[, c('height', 'var_code')]
        id_obs <- id_obs[!duplicated(id_obs, fromLast = TRUE), , drop = FALSE]

        dat_hr <- lapply(seq(nrow(id_obs)), function(j){
            obs_hgt <- id_obs$height[j]
            obs_var <- id_obs$var_code[j]
            ix <- qres$height == obs_hgt & qres$var_code == obs_var
            qvar <- qres[ix, , drop = FALSE]
            if(nrow(qvar) == 0) return(NULL)

            dat_var <- getAWSObs_1hr_TS(qvar, minFrac, tstep[network])

            return(dat_var)
        })

        inull <- sapply(dat_hr, is.null)
        if(all(inull)) next

        dat_hr <- dat_hr[!inull]
        dat_hr <- do.call(rbind, dat_hr)
        name_dat <- names(dat_hr)

        dat_hr <- dat_hr[dat_hr$obs_time >= dlast, , drop = FALSE]
        if(nrow(dat_hr) == 0) next

        ### spatial check here?

        ##### 

        fun_format <- list(as.integer, as.character, as.numeric,
                           as.integer, as.integer, as.integer,
                           as.numeric, as.integer, as.numeric)
        dat_hr <- formatTablesColumns(dat_hr, fun_format, name_dat)
        dat_hr$obs_id <- getObsId(dat_hr)

        ######
        # update last record
        dat_last <- dat_hr[dat_hr$obs_time == dlast, , drop = FALSE]
        if(nrow(dat_last) > 0){
            query <- paste0("SELECT * FROM aws_hourly WHERE network=", network,
                            " AND id='", crds$id[i], "' AND obs_time=", dlast)
            dat_db <- DBI::dbGetQuery(conn, query)
            if(nrow(dat_db) > 0){
                idup <- dat_last$obs_id %in% dat_db$obs_id
                if(any(idup)){
                    dat_last <- dat_last[idup, , drop = FALSE]
                    dat_last$spatial_check[is.na(dat_last$spatial_check)] <- 'NULL'
                    for(j in seq(nrow(dat_last))){
                        statement <- paste0("UPDATE aws_hourly SET value=", dat_last$value[j],
                                            ", spatial_check=", dat_last$spatial_check[j],
                                            ", miss_frac=", dat_last$miss_frac[j],
                                            " WHERE network=", dat_last$network[j],
                                            " AND id='", dat_last$id[j],
                                            "' AND height=", dat_last$height[j],
                                            " AND obs_time=", dat_last$obs_time[j],
                                            " AND var_code=", dat_last$var_code[j],
                                            " AND stat_code=", dat_last$stat_code[j])
                        DBI::dbExecute(conn, statement)
                    }

                    dat_hr <- dat_hr[!dat_hr$obs_id %in% dat_last$obs_id, , drop = FALSE]
                    if(nrow(dat_hr) == 0) next
                }
            }
        }

        #####

        DBI::dbWriteTable(conn, "aws_hourly", dat_hr, append = TRUE, row.names = FALSE)

        last <- max(as.integer(dat_hr$obs_time), na.rm = TRUE)
        statement <- paste0("UPDATE aws_aggr_period SET hour_ts_end=", last,
                            " WHERE network=", network, " AND id='", crds$id[i], "'")
        DBI::dbExecute(conn, statement)
    }

    return(0)
}
