StorageQueue <- R6::R6Class("StorageQueue",

public=list(

    endpoint=NULL,
    name=NULL,

    initialize=function(endpoint, name, ...)
    {
        self$endpoint <- endpoint
        self$name <- name
    },

    create=function()
    {
        do_container_op(self, http_verb="PUT")
        invisible(self)
    },

    delete=function(confirm=TRUE)
    {
        if(!delete_confirmed(confirm, paste0(self$endpoint$url, name), "queue"))
            return(invisible(NULL))

        do_container_op(self, http_verb="DELETE")
        invisible(self)
    },

    clear=function()
    {
        do_container_op(self, "messages", http_verb="DELETE")
    },

    read_message=function() {
        new_message(do_container_op(self, "messages")$QueueMessage, self)
    },

    read_messages=function(n=1) {
        opts <- list(numofmessages=n)
        lapply(do_container_op(self, "messages", options=opts), new_message, queue=self)
    },

    peek_message=function() {
        opts <- list(peekonly=TRUE)
        new_message(do_container_op(self, "messages", options=opts)$QueueMessage, self)
    },

    peek_messages=function(n=1) {
        opts <- list(peekonly=TRUE, numofmessages=n)
        lapply(do_container_op(self, "messages", options=opts), new_message, queue=self)
    },

    pop_message=function() {
        msg <- self$read_message()
        msg$delete()
        msg
    },

    pop_messages=function(n=1) {
        msgs <- self$read_messages(n)
        lapply(msgs, function(msg) msg$delete())
        msgs
    },

    write_message=function(text, visibility_timeout=NULL, time_to_live=NULL)
    {
        text <- if(is.raw(text))
            openssl::base64_encode(text)
        else if(is.character(text))
            paste0(text, collapse="\n")
        else stop("Message text must be raw or character", call.=FALSE)

        opts <- list(visibilitytimeout=visibility_timeout, messagettl=time_to_live)
        body <- paste0("<QueueMessage><MessageText>", text, "</MessageText></QueueMessage>")
        hdrs <- list(`content-length`=sprintf("%.0f", nchar(body)))

        res <- do_container_op(self, "messages", options=opts, headers=hdrs, body=body, http_verb="POST")
        invisible(res$QueueMessage)
    }
))

