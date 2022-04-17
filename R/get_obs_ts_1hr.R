
getAWSObs_1hr_TS <- function(qvar, minFrac, tstep){
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"
    var_out <- c("network", "id", "height", "obs_time",
                 "var_code", "stat_code", "value")

    obs_var <- as.character(qvar$var_code[1])
    all_stat <- unique(qvar$stat_code)

    dat_var <- lapply(all_stat, function(s){
        xval <- qvar[qvar$stat_code == s, , drop = FALSE]

        daty <- as.POSIXct(xval$obs_time, origin = origin, tz = tz)
        times <- format(daty, "%Y%m%d%H%M")
        index <- get_index_min2hour_end(times, 1, tz)

        nobs <- sapply(index, length)
        avail_frac <- as.numeric(nobs)/(60/tstep)

        odaty <- strptime(names(index), "%Y%m%d%H", tz = tz)

        ina <- avail_frac >= minFrac[[obs_var]]
        odaty <- as.numeric(odaty[ina])
        avail_frac <- avail_frac[ina]
        out_dat <- lapply(index[ina], function(iv) xval$value[iv])

        list(stat = s, time = odaty, data = out_dat, frac = avail_frac)
    })

    tabL <- 1:3 %in% all_stat

    dat_var <- lapply(dat_var, function(x){
        xout <- qvar[1, var_out, drop = FALSE]

        xout$stat_code <- x$stat
        xout$value <- NA
        nl <- length(x$time)
        if(nl == 0) return(NULL)

        xout <- xout[rep(1, nl), , drop = FALSE]
        xout$obs_time <- x$time
        xout$spatial_check <- NA
        xout$miss_frac <- x$frac

        xout <- compute_obs_stats(x, xout, tabL)

        return(xout)
    })

    inull <- sapply(dat_var, is.null)
    if(all(inull)) return(NULL)

    dat_var <- dat_var[!inull]
    dat_var <- do.call(rbind, dat_var)
    dat_var <- dat_var[!is.na(dat_var$value), , drop = FALSE]

    return(dat_var)
}
