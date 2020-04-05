#' @import AzureStor
#' @import AzureRMR
NULL


.onLoad <- function(libname, pkgname)
{
    AzureStor::az_storage$set("public", "get_queue_endpoint", overwrite=TRUE,
    function(key=self$list_keys()[1], sas=NULL, token=NULL)
    {
        queue_endpoint(self$properties$primaryEndpoints$queue, key=key, sas=sas, token=token)
    })
}


# assorted imports of unexported functions
delete_confirmed <- getNamespace("AzureStor")$delete_confirmed

is_endpoint_url <- getNamespace("AzureStor")$is_endpoint_url

generate_endpoint_container <- getNamespace("AzureStor")$generate_endpoint_container


# tag xml2 to satisfy R CMD check
xml2::as_xml_document
