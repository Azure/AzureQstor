#' @export
queue_endpoint <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                          api_version=getOption("azure_storage_api_version"))
{
    if(!is_endpoint_url(endpoint, "queue"))
        warning("Not a recognised queue endpoint", call.=FALSE)

    obj <- list(url=endpoint, key=key, token=token, sas=sas, api_version=api_version)
    class(obj) <- c("queue_endpoint", "storage_endpoint")
    obj
}

