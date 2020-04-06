#' R6 class representing an Azure storage queue
#' @description
#' A storage queue holds messages. A queue can contain an unlimited number of messages, each of which can be up to 64KB in size. Messages are generally added to the end of the queue and retrieved from the front of the queue, although first in, first out (FIFO) behavior is not guaranteed.
#'
#' To generate a queue object, use one of the [`storage_queue`], [`list_storage_queues`] or [`create_storage_queue`] functions rather than calling the `new()` method directly.
#'
#' @seealso
#' [`QueueMessage`]
#'
#' @examples
#' \dontrun{
#'
#' endp <- storage_endpoint("https://mystorage.queue.core.windows.net", key="key")
#'
#' # to talk to an existing queue
#' queue <- storage_queue(endp, "queue1")
#'
#' # to create a new queue
#' queue2 <- create_storage_queue(endp, "queue2")
#'
#' # various ways to delete a queue (will ask for confirmation first)
#' queue2$delete()
#' delete_storage_queue(queue2)
#' delete_storage_queue(endp, "queue2")
#'
#' # to get all queues in this storage account
#' queue_lst <- list_storage_queues(endp)
#'
#' # working with a queue: put, get, update and delete messages
#' queue$put_message("new message")
#' msg <- queue$get_message()
#' msg$update(visibility_timeout=60, text="updated message")
#' queue$delete_message(msg)
#'
#' # delete_message simply calls the message's delete() method, so this is equivalent
#' msg$delete()
#'
#' # retrieving multiple messages at a time (up to 32)
#' msgs <- queue$get_messages(30)
#'
#' # deleting is still per-message
#' lapply(msgs, function(m) m$delete())
#'
#' # you can use the process pool from AzureRMR to do this in parallel
#' AzureRMR::init_pool()
#' AzureRMR::pool_lapply(msgs, function(m) m$delete())
#' AzureRMR::delete_pool()
#'
#' }
#' @aliases queue
#' @export
StorageQueue <- R6::R6Class("StorageQueue",

public=list(

    #' @field endpoint A queue endpoint object. This contains the account and authentication information for the queue.
    #' @field name The name of the queue.
    endpoint=NULL,
    name=NULL,

    #' @description
    #' Initialize the queue object. Rather than calling this directly, you should use one of the [`storage_queue`], [`list_storage_queues`] or [`create_storage_queue`] functions.
    #'
    #' Note that initializing this object is a local operation only. If a queue of the given name does not already exist in the storage account, it has to be created remotely by calling the `create` method.
    #'
    #' @param endpoint An endpoint object.
    #' @param name The name of the queue.
    initialize=function(endpoint, name)
    {
        self$endpoint <- endpoint
        self$name <- name
    },

    #' @description
    #' Creates a storage queue in Azure, using the storage endpoint and name from this R6 object.
    create=function()
    {
        do_container_op(self, http_verb="PUT")
        invisible(self)
    },

    #' @description
    #' Deletes this storage queue in Azure.
    #' @param confirm Whether to ask for confirmation before deleting.
    delete=function(confirm=TRUE)
    {
        if(!delete_confirmed(confirm, paste0(self$endpoint$url, name), "queue"))
            return(invisible(NULL))

        do_container_op(self, http_verb="DELETE")
        invisible(self)
    },

    #' @description
    #' Clears (deletes) all messages in this storage queue.
    clear=function()
    {
        do_container_op(self, "messages", http_verb="DELETE")
    },

    #' @description
    #' Reads a message from the front of the storage queue.
    #'
    #' When a message is read, the consumer is expected to process the message and then delete it. After the message is read, it is made invisible to other consumers for a specified interval. If the message has not yet been deleted at the time the interval expires, its visibility is restored, so that another consumer may process it.
    #' @return
    #' A new object of class [`QueueMessage`].
    get_message=function() {
        new_message(do_container_op(self, "messages")$QueueMessage, self)
    },

    #' @description
    #' Reads several messages at once from the front of the storage queue.
    #'
    #' When a message is read, the consumer is expected to process the message and then delete it. After the message is read, it is made invisible to other consumers for a specified interval. If the message has not yet been deleted at the time the interval expires, its visibility is restored, so that another consumer may process it.
    #' @param n How many messages to read. The maximum is 32.
    #' @return
    #' A list of objects of class [`QueueMessage`].
    get_messages=function(n=1) {
        opts <- list(numofmessages=n)
        lapply(do_container_op(self, "messages", options=opts), new_message, queue=self)
    },

    #' @description
    #' Reads a message from the storage queue, but does not alter its visibility.
    #'
    #' Note that a message obtained via the `peek_message` or `peek_messages` method will not include a pop receipt, which is required to delete or update it.
    #' @return
    #' A new object of class [`QueueMessage`].
    peek_message=function() {
        opts <- list(peekonly=TRUE)
        new_message(do_container_op(self, "messages", options=opts)$QueueMessage, self)
    },

    #' @description
    #' Reads several messages at once from the storage queue, without altering their visibility.
    #'
    #' Note that a message obtained via the `peek_message` or `peek_messages` method will not include a pop receipt, which is required to delete or update it.
    #' @param n How many messages to read. The maximum is 32.
    #' @return
    #' A list of objects of class [`QueueMessage`].
    peek_messages=function(n=1) {
        opts <- list(peekonly=TRUE, numofmessages=n)
        lapply(do_container_op(self, "messages", options=opts), new_message, queue=self)
    },

    #' @description
    #' Reads a message from the storage queue, removing it at the same time. This is equivalent to calling [`get_message`](#method-get_message) and [`delete_message`](#method-delete_message) successively.
    #' @return
    #' A new object of class [`QueueMessage`].
    pop_message=function() {
        msg <- self$get_message()
        msg$delete()
        msg
    },

    #' @description
    #' Reads several messages at once from the storage queue, and then removes them.
    #' @param n How many messages to read. The maximum is 32.
    #' @return
    #' A list of objects of class [`QueueMessage`].
    pop_messages=function(n=1) {
        msgs <- self$get_messages(n)
        lapply(msgs, function(msg) msg$delete())
        msgs
    },

    #' @description
    #' Writes a message to the back of the message queue.
    #' @param text The message text, either a raw or character vector. If a raw vector, it is base64-encoded, and if a character vector, it is collapsed into a single string before being sent to the queue.
    #' @param visibility_timeout Optional visibility timeout after being read, in seconds. The default is 30 seconds.
    #' @param time_to_live Optional message time-to-live, in seconds. The default is 7 days.
    put_message=function(text, visibility_timeout=NULL, time_to_live=NULL)
    {
        text <- if(is.raw(text))
            openssl::base64_encode(text)
        else if(is.character(text))
            paste0(text, collapse="\n")
        else stop("Message text must be raw or character", call.=FALSE)

        opts <- list()
        if(!is.null(visibility_timeout))
            opts <- c(opts, visibilitytimeout=visibility_timeout)
        if(!is.null(time_to_live))
            opts <- c(opts, messagettl=time_to_live)
        body <- paste0("<QueueMessage><MessageText>", text, "</MessageText></QueueMessage>")
        hdrs <- list(`content-length`=sprintf("%.0f", nchar(body)))

        res <- do_container_op(self, "messages", options=opts, headers=hdrs, body=body, http_verb="POST")
        invisible(res$QueueMessage)
    },

    #' @description
    #' Updates a message in the queue. This requires that the message object must include a pop receipt, which is present if it was obtained by means other than [peeking](#method-peek_message).
    #'
    #' This operation can be used to continually extend the invisibility of a queue message. This functionality can be useful if you want a worker role to "lease" a message. For example, if a worker role calls [`get_messages`](#method-get_messages) and recognizes that it needs more time to process a message, it can continually extend the message's invisibility until it is processed. If the worker role were to fail during processing, eventually the message would become visible again and another worker role could process it.
    #' @param msg A message object, of class [`QueueMessage`].
    #' @param visibility_timeout The new visibility timeout (time to when the message will again be visible).
    #' @param text Optionally, new message text, either a raw or character vector. If a raw vector, it is base64-encoded, and if a character vector, it is collapsed into a single string before being sent to the queue.
    update_message=function(msg, visibility_timeout, text=msg$text)
    {
        stopifnot(inherits(msg, "QueueMessage"))
        msg$update(visibility_timeout, text)
    },

    #' @description
    #' Deletes a message from the queue. This requires that the message object must include a pop receipt, which is present if it was obtained by means other than [peeking](#method-peek_message).
    #' @param msg A message object, of class [`QueueMessage`].
    delete_message=function(msg)
    {
        stopifnot(inherits(msg, "QueueMessage"))
        msg$delete()
    }
))

