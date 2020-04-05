new_message <- function(message, queue)
{
    message <- unlist(message, recursive=FALSE)
    QueueMessage$new(message, queue)
}


QueueMessage <- R6::R6Class("QueueMessage",

public=list(

    queue=NULL,
    id=NULL,
    insertion_time=NULL,
    expiry_time=NULL,
    text=NULL,
    receipt=NULL,
    next_visible_time=NULL,
    dequeue_count=NULL,

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

    delete=function()
    {
        private$check_receipt()
        opts <- list(popreceipt=self$receipt)
        do_container_op(self$queue, file.path("messages", self$id), options=opts, http_verb="DELETE")
        invisible(NULL)
    },

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
