new_message <- function(message, queue)
{
    message <- unlist(message, recursive=FALSE)
    QueueMessage$new(message, queue)
}


#' R6 class representing a message from an Azure storage queue
#' @export
QueueMessage <- R6::R6Class("QueueMessage",

public=list(

    #' @field queue The queue this message is from, an object of class [`StorageQueue`]
    #' @field id The message ID.
    #' @field insertion_time The message insertion (creation) time.
    #' @field expiry_time The message expiration time.
    #' @field text The message text.
    #' @field receipt A pop receipt. This is populated if the message was retrieved via a `get_message` or `update` call, and is necessary for deleting or further updating the message.
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
    #' Creates a new message object. Rather than calling the `initialize` method manually, objects of this class should be created via the methods exposed by the [`StorageQueue`] object.
    #' @param message Details about the message.
    #' @param queue Object of class `StorageQueue.
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
    #' @param visibility_timeout The new visibility timeout (time to when the message will again be visible).
    #' @param text Optionally, new message text.
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
