#' R6 class representing an Azure storage queue
#' @export
StorageQueue <- R6::R6Class("StorageQueue",

public=list(

    #' @field endpoint A queue endpoint object. This contains the account and authentication information for the queue.
    #' @field name The name of the queue.
    endpoint=NULL,
    name=NULL,

    #' @description
    #' Initialize the queue. Rather than calling this directly, you should use one of the [`storage_queue`], [`list_storage_queues`] or [`create_storage_queue`] functions.
    #' @param endpoint An endpoint object.
    #' @param name The name of the queue.
    #' @details
    #' Initializing this R6 object does not touch the server. If a queue of the given name does not already exist, it has to be created by calling the `create` method.
    initialize=function(endpoint, name)
    {
        self$endpoint <- endpoint
        self$name <- name
    },

    #' @description
    #' Creates a storage queue in Azure, using the storage endpoint and name stored in this R6 object.
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
    #' Reads a message from the front of the storage queue. The message is then marked as read, but must still be deleted manually.
    #' @return
    #' A new object of class [`QueueMessage`].
    get_message=function() {
        new_message(do_container_op(self, "messages")$QueueMessage, self)
    },

    #' @description
    #' Reads several messages at once from the front of the storage queue. The messages are marked as read, but must still be deleted manually.
    #' @param n How many messages to read. The maximum is 32.
    #' @return
    #' A list of objects of class [`QueueMessage`].
    get_messages=function(n=1) {
        opts <- list(numofmessages=n)
        lapply(do_container_op(self, "messages", options=opts), new_message, queue=self)
    },

    #' @description
    #' Reads a message from the storage queue, but does not mark it as read.
    #' @return
    #' A new object of class [`QueueMessage`].
    peek_message=function() {
        opts <- list(peekonly=TRUE)
        new_message(do_container_op(self, "messages", options=opts)$QueueMessage, self)
    },

    #' @description
    #' Reads several messages at once from the storage queue, without marking them as read.
    #' @param n How many messages to read. The maximum is 32.
    #' @return
    #' A list of objects of class [`QueueMessage`].
    peek_messages=function(n=1) {
        opts <- list(peekonly=TRUE, numofmessages=n)
        lapply(do_container_op(self, "messages", options=opts), new_message, queue=self)
    },

    #' @description
    #' Reads a message from the storage queue, and then deletes it.
    #' @return
    #' A new object of class [`QueueMessage`].
    pop_message=function() {
        msg <- self$get_message()
        msg$delete()
        msg
    },

    #' @description
    #' Reads several messages at once from the storage queue, and then deletes them.
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
    #' @param text The message text, either a raw or character vector. If a raw vector, it is base64-encoded, and if a character vector, it is pasted into a single string before being sent to the queue.
    #' @param visibility_timeout Optional visibility timeout after being read, in seconds.
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
    }
))

