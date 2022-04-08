#' Update minutes data database.
#'
#' Update minutes data from AWS to ADT database.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.\cr
#' 
#' @export

update_dataHour_db <- function(aws_dir){
    nb_net <- 2

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
            msg <- paste(ret, "Updating aws_data table failed")
            format.out.msg(paste(mserr, '\n', msg), logPROC)
        }
    }

    DBI::dbDisconnect(conn)

    return(0)
}

update.aws_hourly <- function(conn, dirAWS, network, minFrac){
    netCRDS <- c("adcon_crds", "tahmo_crds")
    netPARS <- c("adcon_pars", "tahmo_pars")
    # netNOM <- c("ADCON", "TAHMO")
    netTSTEP <- c(15, 5)

    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    crds <- DBI::dbReadTable(conn, netCRDS[network])
    lastTb <- DBI::dbReadTable(conn, "aws_aggr_period")

    var_obs <- as.integer(names(minFrac))

    for(i in seq_along(crds$id)){
        last <- lastTb$hour_ts_end[lastTb$network == network & lastTb$id == crds$id[i]]
        if(last == 0){
            last <- as.POSIXct("2015-01-01 00:00:00", tz = tz)
        }else{
            last <- as.POSIXct(as.integer(last), origin = origin, tz = tz) - 60 * 60
        }
        last <- as.integer(last)
        query <- paste0("SELECT * FROM aws_data0 WHERE (network=", network, " AND id='", crds$id[i], "') AND obs_time > ", last)
        qres <- DBI::dbGetQuery(conn, query)
        if(nrow(qres) == 0) next

        id_obs <- which(qres$var_code %in% var_obs)

        ######### 
    }

    #########
}

