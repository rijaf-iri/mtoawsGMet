#' Initialize Table aws_aggr_period.
#'
#' Populate table \code{aws_aggr_period} from \code{adt_db} for hourly and daily data.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

initializeAWSAggrPeriod <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    netInfo <- aws_network_info()
    netCRDS <- netInfo$coords

    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "LOGPROC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "initialize_aws_aggr_period.txt")

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        msg <- "Unable to connect to ADT server\n"
        format.out.msg(msg, logPROC)

        return(1)
    }

    ###########
    crds <- lapply(seq_along(netCRDS), function(j){
        crd <- DBI::dbReadTable(conn, netCRDS[j])
        crd <- crd[, "id", drop = FALSE]
        crd$network <- j
        return(crd)
    })
    crds <- do.call(rbind, crds)

    ###########

    aws_aggr_period <- data.frame(matrix(NA, nrow(crds), 8))
    names(aws_aggr_period) <- c('network', 'id', 'hour_sp_start', 'hour_sp_end',
                                'hour_ts_start', 'hour_ts_end',
                                'day_ts_start', 'day_ts_end')
    aws_aggr_period$network <- crds$network
    aws_aggr_period$id <- crds$id

    ###########
    for(j in seq_along(crds$id)){
        query <- paste0("SELECT min(obs_time) as mn, max(obs_time) as mx",
                        " FROM aws_data WHERE ", 
                        "network=", crds$network[j], " AND id='", crds$id[j], "'")
        qres <- DBI::dbGetQuery(conn, query)
        if(nrow(qres) == 0) next
        aws_aggr_period$hour_sp_start[j] <- qres$mn
        aws_aggr_period$hour_sp_end[j] <- qres$mx
    }

    for(j in seq_along(crds$id)){
        query <- paste0("SELECT min(obs_time) as mn, max(obs_time) as mx",
                        " FROM aws_hourly WHERE ", 
                        "network=", crds$network[j], " AND id='", crds$id[j], "'")
        qres <- DBI::dbGetQuery(conn, query)
        if(nrow(qres) == 0) next
        aws_aggr_period$hour_ts_start[j] <- qres$mn
        aws_aggr_period$hour_ts_end[j] <- qres$mx
    }

    for(j in seq_along(crds$id)){
        query <- paste0("SELECT min(obs_time) as mn, max(obs_time) as mx",
                        " FROM aws_daily WHERE ", 
                        "network=", crds$network[j], " AND id='", crds$id[j], "'")
        qres <- DBI::dbGetQuery(conn, query)
        if(nrow(qres) == 0) next
        aws_aggr_period$day_ts_start[j] <- qres$mn
        aws_aggr_period$day_ts_end[j] <- qres$mx
    }

    ###########

    fun_format <- list(as.integer, as.character, as.integer, as.integer,
                       as.integer, as.integer, as.integer, as.integer)
    name_col <- names(aws_aggr_period)
    aws_aggr_period <- lapply(seq_along(fun_format), function(i) fun_format[[i]](aws_aggr_period[[i]]))
    aws_aggr_period <- as.data.frame(aws_aggr_period)
    names(aws_aggr_period) <- name_col

    DBI::dbWriteTable(conn, "aws_aggr_period", aws_aggr_period, append = TRUE, row.names = FALSE)

    return(0)
}
