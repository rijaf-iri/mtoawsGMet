#' Create AWS variables JSON file.
#'
#' Create AWS variables JSON file.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

get_aws_variables <- function(aws_dir){
    file_pars <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_parameters.json")
    json <- jsonlite::read_json(file_pars)

    PARS <- lapply(json, function(js){
        stn <- lapply(js$PARS, function(p){
            p <- as.character(p)
            vars <- data.frame(js$PARS_Info[[p]][[1]])
            names(vars) <- paste0("var_", names(vars))

            stats <- js$STAT[[p]]
            stats <- lapply(seq_along(stats), function(s){
                height <- as.numeric(names(stats[s]))
                ost <- lapply(stats[[s]], data.frame)
                ost <- do.call(rbind, ost)
                names(ost) <- paste0("stat_", names(ost))

                cbind(height, ost)
            })
            stats <- do.call(rbind, stats)

            cbind(vars, stats)
        })
        do.call(rbind, stn)
    })

    PARS <- do.call(rbind, PARS)
    PARS <- PARS[!duplicated(PARS[, c('var_code', 'height', 'stat_code')]), ]

    json <- convJSON(PARS)

    jsonfile <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_variables.json")
    unlink(jsonfile)
    cat(json, file = jsonfile)

    return(0)
}
