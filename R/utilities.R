
Sys.setenv(TZ = "Africa/Accra")

char_utc2local_time <- function(dates, format, tz){
    x <- strptime(dates, format, tz = "UTC")
    x <- as.POSIXct(x)
    x <- format(x, format, tz = tz)
    x <- strptime(x, format, tz = tz)
    as.POSIXct(x)
}

time_utc2local_char <- function(dates, format, tz){
    x <- as.POSIXct(dates)
    x <- format(x, format, tz = tz)
    x
}

char_local2utc_time <- function(dates, format, tz){
    x <- strptime(dates, format, tz = tz)
    x <- as.POSIXct(x)
    x <- format(x, format, tz = "UTC")
    x <- strptime(x, format, tz = "UTC")
    as.POSIXct(x)
}

time_local2utc_char <- function(dates, format){
    x <- as.POSIXct(dates)
    x <- format(x, format, tz = "UTC")
    x
}

time_local2utc_time <- function(dates){
    format <- "%Y-%m-%d %H:%M:%S"
    x <- time_local2utc_char(dates, format)
    x <- strptime(x, format, tz = "UTC")
    as.POSIXct(x)
}

time_utc2time_local <- function(dates, tz){
    format <- "%Y-%m-%d %H:%M:%S"
    x <- time_utc2local_char(dates, format, tz)
    x <- strptime(x, format, tz = tz)
    as.POSIXct(x)
}

####################################################
## Time at the end
## 2018-01-01 00:00 will represent the average or total from
## (2018-01-01 00:00)-out.step to 2018-01-01 00:00

get_index_min2min_end <- function(times, out.step, tz){
    ttn <- as.numeric(substr(times, 11, 12))
    ttn <- ttn - 1/60
    ttn <- floor(ttn / out.step) * out.step
    ttn <- (ttn + out.step) %% 60
    ttn <- stringr::str_pad(ttn, 2, pad = "0")
    trle <- rle(ttn)
    ie <- cumsum(trle$lengths)
    is <- c(1, (ie + 1)[-length(ie)])
    index <- lapply(seq_along(is), function(j) is[j]:ie[j])

    tth <- times[ie]
    step <- !substr(tth, 11, 12) %in% unique(trle$values)
    if(any(step)){
        td <- as.POSIXct(tth[step], tz = tz, format = "%Y%m%d%H%M")
        tth[step] <- paste0(format(td + out.step * 60, "%Y%m%d%H"), trle$values[step])
    }
    names(index) <- tth

    return(index)
}

get_index_min2hour_end <- function(times, out.step, tz){
    times <- substr(times, 1, 12)
    ttn <- as.POSIXct(times, tz = tz, format = "%Y%m%d%H%M")
    ttn <- ttn - 1
    ttn <- format(ttn, "%Y%m%d%H%M")
    index <- split(seq_along(ttn), substr(ttn, 1, 10))

    tth <- names(index)
    tth <- as.POSIXct(tth, tz = tz, format = "%Y%m%d%H")
    tth <- format(tth + 3600, "%Y%m%d%H")
    names(index) <- tth

    if(out.step > 1){
        idx <- get_index_hour2hour_end(tth, out.step, tz)
        index <- lapply(idx, function(j) unlist(index[j], use.names = FALSE))
    }

    return(index)
}

get_index_hour2hour_end <- function(times, out.step, tz){
    ttn <- as.numeric(substr(times, 9, 10))
    ttn <- ttn - 1/60
    ttn <- floor(ttn / out.step) * out.step
    ttn <- (ttn + out.step) %% 24
    ttn <- stringr::str_pad(ttn, 2, pad = "0")
    trle <- rle(ttn)
    ie <- cumsum(trle$lengths)
    is <- c(1, (ie + 1)[-length(ie)])
    index <- lapply(seq_along(is), function(j) is[j]:ie[j])

    tth <- times[ie]
    step <- !substr(tth, 9, 10) %in% unique(trle$values)
    if(any(step)){
        td <- as.POSIXct(tth[step], tz = tz, format = "%Y%m%d%H")
        tth[step] <- paste0(format(td + out.step * 3600, "%Y%m%d"), trle$values[step])
    }
    names(index) <- tth

    return(index)
}

get_index_minhour2day_end <- function(times, instep, obs.hour, tz){
    it <- switch(instep, 'minute' = 12, 'hourly' = 10)
    times <- substr(times, 1, it)
    format <- switch(instep, 'minute' = "%Y%m%d%H%M", 'hourly' = "%Y%m%d%H")
    ttn <- as.POSIXct(times, tz = tz, format = format)
    ttn <- (ttn - 1) - 3600 * obs.hour
    ttn <- format(ttn, format)
    index <- split(seq_along(ttn), substr(ttn, 1, 8))

    return(index)
}

## Time at the beginning
## 2018-01-01 00:00 will represent the average or total from
## 2018-01-01 00:00 to (2018-01-01 00:00)+out.step

get_index_min2min_start <- function(times, out.step){
    ttn <- as.numeric(substr(times, 11, 12))
    ttn <- floor(ttn / out.step) * out.step
    ttn <- stringr::str_pad(ttn, 2, pad = "0")
    index <- split(seq_along(ttn), paste0(substr(times, 1, 10), ttn))

    return(index)
}

get_index_min2hour_start <- function(times, out.step){
    index <- split(seq_along(times), substr(times, 1, 10))
    if(out.step > 1){
        idx <- get_index_hour2hour_start(names(index), out.step)
        index <- lapply(idx, function(j) unlist(index[j], use.names = FALSE))
    }

    return(index)
}

get_index_hour2hour_start <- function(times, out.step){
    ttn <- as.numeric(substr(times, 9, 10))
    ttn <- floor(ttn / out.step) * out.step
    ttn <- stringr::str_pad(ttn, 2, pad = "0")
    index <- split(seq_along(ttn), paste0(substr(times, 1, 8), ttn))

    return(index)
}

get_index_minhour2day_start <- function(times, instep, obs.hour, tz){
    it <- switch(instep, 'minute' = 12, 'hourly' = 10)
    times <- substr(times, 1, it)
    format <- switch(instep, 'minute' = "%Y%m%d%H%M", 'hourly' = "%Y%m%d%H")
    ttn <- as.POSIXct(times, tz = tz, format = format)
    ttn <- ttn  - 3600 * obs.hour
    ttn <- format(ttn, format)
    index <- split(seq_along(ttn), substr(ttn, 1, 8))

    return(index)
}

####################################################

connect.database <- function(con_args, drv){
    args <- c(list(drv = drv), con_args)
    con <- do.call(DBI::dbConnect, args)
    con
}

connect_MySQL <- function(dirAWS, fileCON){
    adt <- readRDS(file.path(dirAWS, "AWS_DATA", "AUTH", fileCON))
    conn <- try(connect.database(adt$connection,
                RMySQL::MySQL()), silent = TRUE)
    if(inherits(conn, "try-error")){
        Sys.sleep(1)
        conn <- try(connect.database(adt$connection,
                    RMySQL::MySQL()), silent = TRUE)
        if(inherits(conn, "try-error")) return(NULL)
    }

    DBI::dbExecute(conn, "SET GLOBAL local_infile=1")
    return(conn)
}

connect_RPostgres <- function(dirAWS, fileCON){
    con_args <- readRDS(file.path(dirAWS, "AWS_DATA", "AUTH", fileCON))
    conn <- try(connect.database(con_args$connection,
                RPostgres::Postgres()), silent = TRUE)
    if(inherits(conn, "try-error")){
        Sys.sleep(3)
        conn <- try(connect.database(con_args$connection,
                    RPostgres::Postgres()), silent = TRUE)
        if(inherits(conn, "try-error")) return(NULL)
    }

    return(conn)
}

####################################################

getObsId <- function(qres){
    paste(qres$network, qres$id, qres$height,
          qres$obs_time, qres$var_code,
          qres$stat_code, sep = "_")
}

formatTablesColumns <- function(qres, format, name){
    out <- lapply(seq_along(format), function(i) format[[i]](qres[[i]]))
    out <- as.data.frame(out)
    names(out) <- name
    out
}

deleteDuplicatedObs <- function(conn, table_name, obs_id){
    del_obs <- split(obs_id, ceiling(seq_along(obs_id) / 100))
    lapply(del_obs, function(id){
        vec <- paste0("'", id, "'")
        vec <- paste0(vec, collapse = ", ")
        vec <- paste0("(", vec, ")")
        ## replace clause IN with JOIN INNER
        ## create global temporary table with column obs_id
        query <- paste0("DELETE FROM ", table_name, " WHERE obs_id IN ", vec)
        DBI::dbExecute(conn, query)
    })
}

####################################################

compute_var_statS <- function(datList, fun){
    out <- sapply(datList, function(v){
        if(all(is.na(v))) return(NA)
         fun(v, na.rm = TRUE)   
    })

    unname(out)
}

compute_var_statM <- function(datList, fun_var, name_var){
    out0 <- lapply(name_var, function(n) NA)
    names(out0) <- name_var

    out <- lapply(datList, function(v){
        if(all(is.na(v))) return(out0)
        vv <- lapply(seq_along(name_var), function(n){
            fun_var[[n]](v, na.rm = TRUE)
        })
        names(vv) <- name_var
        vv
    })

    out <- lapply(seq_along(name_var), function(i){
        vv <- sapply(out, '[[', name_var[i])
        unname(vv)
    })
    names(out) <- name_var

    out
}

format_var_statM <- function(datList, fun_var, name_var, stat_var, out){
    vout <- compute_var_statM(datList, fun_var, name_var)

    xout <- lapply(seq_along(name_var), function(i){
        v <- out
        v$stat_code <- stat_var[i]
        v$value <- vout[[name_var[i]]]
        v
    })

    do.call(rbind, xout)
}

####################################################

format.out.msg <- function(msg, logfile, append = TRUE){
    ret <- c(paste("Time:", Sys.time(), "\n"), msg, "\n",
             "*********************************\n")
    cat(ret, file = logfile, append = append)
}
