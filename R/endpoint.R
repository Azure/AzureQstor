#' Create a queue endpoint object
#'
#' @param endpoint The URL (hostname) for the endpoint, of the form `http[s]://{account-name}.queue.{core-host-name}`. On the public Azure cloud, endpoints will be of the form `https://{account-name}.queue.core.windows.net`.
#' @param key The access key for the storage account.
#' @param token An Azure Active Directory (AAD) authentication token. This can be either a string, or an object of class AzureToken created by [AzureRMR::get_azure_token]. The latter is the recommended way of doing it, as it allows for automatic refreshing of expired tokens.
#' @param sas A shared access signature (SAS) for the account.
#' @param api_version The storage API version to use when interacting with the host. Defaults to `"2019-07-07"`.
#'
#' @details
#' This is the queue storage counterpart to the endpoint functions defined in the AzureStor package.
#' @return
#' An object of class `queue_endpoint`, inheriting from `storage_endpoint`.
#' @seealso
#' [`AzureStor::storage_endpoint`], [`AzureStor::blob_endpoint`], [`storage_queue`]
#' @examples
#' \dontrun{
#'
#' # obtaining an endpoint from the storage account resource object
#' AzureRMR::get_azure_login()$
#'     get_subscription("sub_id")$
#'     get_resource_group("rgname")$
#'     get_storage_account("mystorage")$
#'     get_queue_endpoint()
#'
#' # creating an endpoint standalone
#' queue_endpoint("https://mystorage.queue.core.windows.net/", key="access_key")
#'
#' }
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

