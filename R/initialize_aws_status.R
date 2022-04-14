#' Initialize AWS status.
#'
#' Create an initial AWS status rds file.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.\cr
#' 
#' @export

initializeAWSStatus <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    netInfo <- aws_network_info()
    tstep <- netInfo$tstep
    netNOM <- netInfo$names
    netCRDS <- netInfo$coords

    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    nmCol <- c("id", "name", "longitude", "latitude", "altitude",
               "Region", "District", "startdate", "enddate")

    ###########
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "LOGPROC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "aws_status.txt")

    dirSTATUS <- file.path(aws_dir, "AWS_DATA", "STATUS")
    if(!dir.exists(dirSTATUS))
        dir.create(dirSTATUS, showWarnings = FALSE, recursive = TRUE)

    ###########
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        msg <- "Unable to connect to ADT server\n"
        format.out.msg(msg, logPROC)

        return(1)
    }

    ###########
    crds <- lapply(seq_along(netCRDS), function(j){
        crd <- DBI::dbReadTable(conn, netCRDS[j])
        crd <- crd[, nmCol, drop = FALSE]
        crd$timestep <- tstep[j]
        crd$network <- netNOM[j]
        return(crd)
    })
    crds <- do.call(rbind, crds)

    startdate <- as.POSIXct(as.integer(crds$startdate), origin = origin, tz = tz)
    enddate <- as.POSIXct(as.integer(crds$enddate), origin = origin, tz = tz)

    ###########
    last <- max(enddate[!is.na(enddate)])
    last0 <- last

    daty <- format(last0, "%Y%m%d%H")
    daty <- strptime(daty, "%Y%m%d%H", tz = tz)

    ###########
    last <- as.POSIXct(last)
    times <- last - 30 * 24 * 60 * 60
    time_hr <- seq(times, last, "hour")
    time_fmt <- format(time_hr, '%Y%m%d%H')
    time0 <- as.numeric(times)

    query <- paste0("SELECT network, id, obs_time FROM aws_data0 WHERE obs_time>=", time0)
    qres <- DBI::dbGetQuery(conn, query)

    ###########
    ix <- split(seq(nrow(qres)), qres$id)
    val <- lapply(ix, function(i){
        n <- unique(qres$network[i])
        y <- sort(unique(qres$obs_time[i]))
        y <- as.POSIXct(y, origin = origin, tz = tz)
        it <- split(seq_along(y), format(y, '%Y%m%d%H'))
        nl <- sapply(it, function(v){
                round(100*length(v)/(60/tstep[n]), 1)
        })
        ii <- match(time_fmt, names(nl))

        nl <- unname(nl[ii])
        nl[is.na(nl)] <- 0
        nl
    })

    stnID <- names(val)
    val <- do.call(rbind, val)

    ix <- match(crds$id, stnID)
    val <- val[ix, , drop = FALSE]
    val[is.na(val)] <- 0
    dimnames(val) <- NULL

    ###############
    crds$startdate <- format(startdate, "%Y-%m-%d %H:%M:%S")
    crds$enddate <- format(enddate, "%Y-%m-%d %H:%M:%S")

    aws <- list(coords = crds, time = time_hr[-1],
                status = val[, -1, drop = FALSE],
                actual_time = daty, updated = Sys.time())

    saveRDS(aws, file = file.path(dirSTATUS, "aws_status.rds"))
}
