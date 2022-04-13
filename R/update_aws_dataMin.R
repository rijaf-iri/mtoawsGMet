#' Update minutes data database.
#'
#' Update minutes data from AWS to ADT database.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.\cr
#' 
#' @export

update_dataMin_db <- function(aws_dir){
    nb_net <- 3

    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "LOGPROC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "update_aws_data0.txt")

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        msg <- "Unable to connect to ADT server\n"
        format.out.msg(msg, logPROC)

        return(1)
    }

    for(n in 1:nb_net){
        ret <- try(update.aws_data0(conn, aws_dir, n), silent = TRUE)
        if(inherits(ret, "try-error")){ 
            mserr <- gsub('[\r\n]', '', ret[1])
            msg <- paste(ret, "Updating aws_data0 table failed")
            format.out.msg(paste(mserr, '\n', msg), logPROC)
        }
    }

    DBI::dbDisconnect(conn)

    return(0)
}

update.aws_data0 <- function(conn, dirAWS, network){
    netDIR <- c("ADCON_SYNOP", "ADCON_AWS", "TAHMO")
    netCRDS <- c("adcon_synop_crds", "adcon_aws_crds", "tahmo_crds")

    ######
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    dirRDS <- file.path(dirAWS, "AWS_DATA/DATA", netDIR[network])
    allAWS0 <- list.files(dirRDS, "*")
    if(length(allAWS0) == 0) return(NULL)

    allAWS <- strsplit(allAWS0, "_")
    len <- sapply(allAWS, 'length')
    ilen <- len == 3
    if(!any(ilen)) return(NULL)

    allAWS0 <- allAWS0[ilen]
    allAWS <- allAWS[ilen]
    allAWS <- do.call(rbind, allAWS)

    crds <- DBI::dbReadTable(conn, netCRDS[network])

    for(i in seq_along(crds$id)){
        ix <- allAWS[, 1] == crds$id[i]
        if(!any(ix)) next
        awsList <- allAWS0[ix]
        last <- as.POSIXct(as.integer(crds$enddate[i]), origin = origin, tz = tz)
        start <- as.POSIXct(as.integer(allAWS[ix, 2]), origin = origin, tz = tz)

        it <- start > last
        if(!any(it)) next
        awsList <- awsList[it]
        dat <- lapply(awsList, function(x){
            readRDS(file.path(dirRDS, x))
        })
        dat <- do.call(rbind, dat)

        last <- as.integer(max(dat$obs_time, na.rm = TRUE))

        DBI::dbWriteTable(conn, "aws_data0", dat, append = TRUE, row.names = FALSE)

        statement <- paste0("UPDATE ", netCRDS[network], " SET enddate=", last,
                            " WHERE id='", crds$id[i], "'")
        DBI::dbExecute(conn, statement)
    }

    return(0)
}
