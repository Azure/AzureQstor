new_message <- function(message, queue)
{
    message <- unlist(message, recursive=FALSE)
    QueueMessage$new(message, queue)
}


#' R6 class representing a message from an Azure storage queue
#' @description
#' This class stores the data, metadata and behaviour associated with a message.
#'
#' To generate a message object, call one of the methods exposed by the [`StorageQueue`] class.
#' @examples
#' \dontrun{
#'
#' endp <- storage_endpoint("https://mystorage.queue.core.windows.net", key="key")
#' queue <- storage_queue(endp, "queue1")
#'
#' msg <- queue$get_message()
#' msg$update(visibility_timeout=60, text="updated message")
#' msg$delete()
#'
#' }
#' @aliases message
#' @export
QueueMessage <- R6::R6Class("QueueMessage",

public=list(

    #' @field queue The queue this message is from, an object of class [`StorageQueue`]
    #' @field id The message ID.
    #' @field insertion_time The message insertion (creation) time.
    #' @field expiry_time The message expiration time.
    #' @field text The message text.
    #' @field receipt A pop receipt. This is present if the message was obtained by means other than [peeking][StorageQueue], and is required for updating or deleting the message.
    #' @field next_visible_time The time when this message will be next visible.
    #' @field dequeue_count The number of times this message has been read.
    queue=NULL,
    id=NULL,
    insertion_time=NULL,
    expiry_time=NULL,
    text=NULL,
    receipt=NULL,
    next_visible_time=NULL,
    dequeue_count=NULL,

    #' @description
    #' Creates a new message object. Rather than calling the `new` method manually, objects of this class should be created via the methods exposed by the [`StorageQueue`] object.
    #' @param message Details about the message.
    #' @param queue Object of class `StorageQueue`.
    initialize=function(message, queue)
    {
        self$queue <- queue
        self$id <- message$MessageId
        self$insertion_time <- message$InsertionTime
        self$expiry_time <- message$ExpirationTime
        self$text <- message$MessageText
        self$receipt <- message$PopReceipt
        self$next_visible_time <- message$TimeNextVisible
        self$dequeue_count <- message$DequeueCount
    },

    #' @description
    #' Deletes this message from the queue.
    delete=function()
    {
        private$check_receipt()
        opts <- list(popreceipt=self$receipt)
        do_container_op(self$queue, file.path("messages", self$id), options=opts, http_verb="DELETE")
        invisible(NULL)
    },

    #' @description
    #' Updates this message in the queue.
    #'
    #' This operation can be used to continually extend the invisibility of a queue message. This functionality can be useful if you want a worker role to "lease" a message. For example, if a worker role calls [`get_messages`][StorageQueue] and recognizes that it needs more time to process a message, it can continually extend the message's invisibility until it is processed. If the worker role were to fail during processing, eventually the message would become visible again and another worker role could process it.
    #' @param visibility_timeout The new visibility timeout (time to when the message will again be visible).
    #' @param text Optionally, new message text, either a raw or character vector. If a raw vector, it is base64-encoded, and if a character vector, it is collapsed into a single string before being sent to the queue.
    update=function(visibility_timeout, text=self$text)
    {
        private$check_receipt()
        text <- if(is.raw(text))
            openssl::base64_encode(text)
        else if(is.character(text))
            paste0(text, collapse="\n")
        else stop("Message text must be raw or character", call.=FALSE)

        opts <- list(popreceipt=self$receipt, visibilitytimeout=visibility_timeout)
        body <- paste0("<QueueMessage><MessageText>", text, "</MessageText></QueueMessage>")
        hdrs <- list(`content-length`=sprintf("%.0f", nchar(body)))

        res <- do_container_op(self$queue, file.path("messages", self$id), options=opts, headers=hdrs, body=body,
                               http_verb="PUT", return_headers=TRUE)

        self$receipt <- res$`x-ms-popreceipt`
        self$next_visible_time <- res$`x-ms-next-visible-time`
        self$text <- text
        invisible(self)
    }
),

private=list(

    check_receipt=function()
    {
        if(is_empty(self$receipt))
            stop("Must have a pop receipt to perform this operation", call.=FALSE)
    }
))
