#' Create AWS coordinates and parameters JSON file.
#'
#' Create AWS coordinates and parameters JSON file.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

get_aws_parameters <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn))
        stop("Unable to connect to ADT server\n")

    netInfo <- aws_network_info()
    netNOM <- tolower(netInfo$names)
    netDIR <- netInfo$dirs
    netCRDS <- netInfo$coords
    netPARS <- netInfo$pars

    var_col <- c("var_name", "var_height", "var_unit",
                 "var_stat", "var_code", "stat_code")
    var_n <- c('var_code', 'var_name', 'var_unit')
    aws_n <- c('id', 'name', 'longitude', 'latitude',
               'altitude', 'Region', 'District')

    pars_net <- lapply(seq_along(netNOM), function(network){
        crd_tab <- DBI::dbReadTable(conn, netCRDS[network])
        var_tab <- DBI::dbReadTable(conn, netPARS[network])
        var_tab <- var_tab[, var_col, drop = FALSE]
        var_tab <- var_tab[!duplicated(var_tab), , drop = FALSE]

        ############

        dirRDS <- file.path(aws_dir, "AWS_DATA/DATA0", netDIR[network])
        allAWS <- list.files(dirRDS, ".+\\.rds$")
        allAWS <- gsub(".rds", "", allAWS)

        pars <- lapply(allAWS, function(aws){
            dat <- readRDS(file.path(dirRDS, paste0(aws, ".rds")))
            var_code <- unique(dat$var_code)
            vars <- lapply(var_code, function(v){
                stat <- dat[dat$var_code == v, , drop = FALSE]
                stat <- stat[, c('height', 'stat_code')]
                stat <- stat[!duplicated(stat, fromLast = TRUE), , drop = FALSE]
                stat <- split(stat$stat_code, stat$height)
                stat <- lapply(stat, sort)
                stat_tab <- var_tab[var_tab$var_code == v, var_col, drop = FALSE]
                out_stat <- lapply(seq_along(stat), function(i){
                    hgt <- as.numeric(names(stat[i]))
                    st <- stat_tab[stat_tab$var_height == hgt, , drop = FALSE]
                    is <- match(stat[[i]], st$stat_code)
                    list(height = hgt,
                         stat = data.frame(code = stat[[i]], name = st$var_stat[is])
                       )
                })

                height <- lapply(out_stat, '[[', 'height')
                height <- as.data.frame(height)
                names(height) <- height

                stat <- lapply(out_stat, '[[', 'stat')
                names(stat) <- height

                vv <- var_tab[var_tab$var_code == v, var_n, drop = FALSE]
                names(vv) <- c('code', 'name', 'units')
                vv$units[is.na(vv$units)] <- ""
                
                list(var = vv[1, , drop = FALSE], height = height, stat = stat)
            })
            names(vars) <- var_code

            var_info <- lapply(vars, '[[', 'var')
            height <- lapply(vars, '[[', 'height')
            stat <- lapply(vars, '[[', 'stat')

            out <- list(as.matrix(crd_tab[crd_tab$id == aws, aws_n]), 
                        PARS = list(var_code),
                        PARS_Info = list(var_info),
                        height = list(height),
                        STATS = list(stat)
                    )

            do.call(cbind, out)
        })

        pars <- do.call(rbind, pars)
        net_info <- data.frame(network_code = network,
                               network = netNOM[network])
        net_info[seq(nrow(pars)), ] <- net_info

        cbind(as.matrix(net_info), pars)
    })

    pars_net <- do.call(rbind, pars_net)
    pars_net <- as.data.frame(pars_net)
    rownames(pars_net) <- NULL
    json <- convJSON(pars_net)

    jsonfile <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_parameters.json")
    unlink(jsonfile)
    cat(json, file = jsonfile)

    return(0)
}
