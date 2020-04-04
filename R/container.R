#' Operations on a queue endpoint
#'
#' Get, list, create, or delete queues.
#'
#' @param endpoint Either a queue endpoint object as created by [storage_endpoint], or a character string giving the URL of the endpoint.
#' @param key,token,sas If an endpoint object is not supplied, authentication credentials: either an access key, an Azure Active Directory (AAD) token, or a SAS, in that order of priority.
#' @param api_version If an endpoint object is not supplied, the storage API version to use when interacting with the host. Currently defaults to `"2019-07-07"`.
#' @param name The name of the queue to get, create, or delete.
#' @param confirm For deleting a queue, whether to ask for confirmation.
#' @param x For the print method, a queue object.
#' @param ... Further arguments passed to lower-level functions.
#' @export
queue <- function(endpoint, ...)
{
    UseMethod("queue")
}

#' @rdname queue
#' @export
queue.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                            api_version=getOption("azure_storage_api_version"),
                            ...)
{
    do.call(queue, generate_endpoint(endpoint, key, token, sas, api_version))
}

#' @rdname queue
#' @export
queue.queue_endpoint <- function(endpoint, name, ...)
{
    obj <- list(name=name, endpoint=endpoint)
    class(obj) <- c("queue", "storage_container")
    obj
}

#' @rdname queue
#' @export
print.queue <- function(x, ...)
{
    cat("Azure queue '", x$name, "'\n", sep="")
    url <- httr::parse_url(x$endpoint$url)
    url$path <- x$name
    cat(sprintf("URL: %s\n", httr::build_url(url)))

    if(!is_empty(x$endpoint$key))
        cat("Access key: <hidden>\n")
    else cat("Access key: <none supplied>\n")

    if(!is_empty(x$endpoint$token))
    {
        cat("Azure Active Directory access token:\n")
        print(x$endpoint$token)
    }
    else cat("Azure Active Directory access token: <none supplied>\n")

    if(!is_empty(x$endpoint$sas))
        cat("Account shared access signature: <hidden>\n")
    else cat("Account shared access signature: <none supplied>\n")

    cat(sprintf("Storage API version: %s\n", x$endpoint$api_version))
    invisible(x)
}


#' @rdname queue
#' @export
list_queues <- function(endpoint, ...)
{
    UseMethod("list_queues")
}

#' @rdname queue
#' @export
list_queues.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                  api_version=getOption("azure_storage_api_version"),
                                  ...)
{
    do.call(list_queues, generate_endpoint_container(endpoint, key, token, sas, api_version))
}

#' @rdname queue
#' @export
list_queues.queue_endpoint <- function(endpoint, ...)
{
    res <- call_storage_endpoint(endpoint, "/", options=list(comp="list"))
    lst <- lapply(res$Queue, function(cont) queue(endpoint, cont$Name[[1]]))

    while(length(res$NextMarker) > 0)
    {
        res <- call_storage_endpoint(endpoint, "/", options=list(comp="list", marker=res$NextMarker[[1]]))
        lst <- c(lst, lapply(res$Queue, function(cont) queue(endpoint, cont$Name[[1]])))
    }
    named_list(lst)
}

#' @rdname queue
#' @export
list_storage_containers.queue <- function(endpoint, ...)
list_queues(endpoint, ...)



#' @rdname queue
#' @export
create_queue <- function(endpoint, ...)
{
    UseMethod("create_queue")
}

#' @rdname queue
#' @export
create_queue.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                   api_version=getOption("azure_storage_api_version"),
                                   ...)
{
    endp <- generate_endpoint_container(endpoint, key, token, sas, api_version)
    create_queue(endp$endpoint, endp$name, ...)
}

#' @rdname queue
#' @export
create_queue.queue <- function(endpoint, ...)
{
    create_queue(endpoint$endpoint, endpoint$name)
}

#' @rdname queue
#' @export
create_queue.queue_endpoint <- function(endpoint, name, ...)
{
    obj <- queue(endpoint, name)
    do_container_op(obj, http_verb="PUT")
    obj
}



#' @rdname queue
#' @export
delete_queue <- function(endpoint, ...)
{
    UseMethod("delete_queue")
}

#' @rdname queue
#' @export
delete_queue.character <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                                             api_version=getOption("azure_storage_api_version"),
                                             ...)
{
    endp <- generate_endpoint_container(endpoint, key, token, sas, api_version)
    delete_queue(endp$endpoint, endp$name, ...)
}

#' @rdname queue
#' @export
delete_queue.queue <- function(endpoint, ...)
{
    delete_queue(endpoint$endpoint, endpoint$name, ...)
}

#' @rdname queue
#' @export
delete_queue.queue_endpoint <- function(endpoint, name, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, paste0(endpoint$url, name), "queue"))
        return(invisible(NULL))

    obj <- queue(endpoint, name)
    invisible(do_container_op(obj, http_verb="DELETE"))
}
