
aws_network_info <- function(){
    list(
        nbnet = 3,
        tstep = c(15, 15, 5),
        names = c("Adcon_Synop", "Adcon_AWS", "Tahmo"),
        coords = c("adcon_synop_crds", "adcon_aws_crds", "tahmo_crds"),
        pars = c("adcon_synop_pars", "adcon_aws_pars", "tahmo_pars"),
        dirs = c("ADCON_SYNOP", "ADCON_AWS", "TAHMO")
    )
}
