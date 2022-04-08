
getAWSObs_1hr_SP <- function(qvar){
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"
    var_out <- c("network", "id", "height", "obs_time",
                 "var_code", "stat_code", "value")

    all_stat <- unique(qvar$stat_code)

    dat_var <- lapply(all_stat, function(s){
        xval <- qvar[qvar$stat_code == s, , drop = FALSE]

        daty <- as.POSIXct(xval$obs_time, origin = origin, tz = tz)
        times <- format(daty, "%Y%m%d%H%M%S")

        # index <- get_index_min2min_end(times, 30, tz)
        # odaty <- strptime(names(index), "%Y%m%d%H%M", tz = tz)
        index <- get_index_min2hour_end(times, 1, tz)
        odaty <- strptime(names(index), "%Y%m%d%H", tz = tz)

        odaty <- as.numeric(odaty)
        out_dat <- lapply(index, function(iv) xval$value[iv])

        list(stat = s, time = odaty, data = out_dat)
    })

    tabL <- 1:3 %in% all_stat

    dat_var <- lapply(dat_var, function(x){
        xout <- qvar[1, var_out, drop = FALSE]

        xout$stat_code <- x$stat
        xout$value <- NA
        xout <- xout[rep(1, length(x$time)), , drop = FALSE]
        xout$obs_time <- x$time
        xout$limit_check <- NA

        xout <- compute_obs_stats(x, xout, tabL)

        return(xout)
    })

    dat_var <- do.call(rbind, dat_var)
    dat_var <- dat_var[!is.na(dat_var$value), , drop = FALSE]

    return(dat_var)
}