#' @export
get_storage_metadata.StorageQueue <- function(object, ...)
{
    object$get_metadata()
}

#' @export
set_storage_metadata.StorageQueue <- function(object, ..., keep_existing=TRUE)
{
    object$set_metadata(..., keep_existing=keep_existing)
}
