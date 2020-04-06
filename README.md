# AzureQstor

An R interface to Azure queue storage, building on the functionality provided by [AzureStor](https://github.com/Azure/AzureStor).

Azure queue storage is a service for storing large numbers of messages that can be accessed from anywhere in the world via authenticated calls using HTTP or HTTPS. A single queue message can be up to 64 KB in size, and a queue can contain millions of messages, up to the total capacity limit of a storage account. Queue storage is often used to create a backlog of work to process asynchronously.

AzureQstor uses a combination of S3 and R6 classes. The queue endpoint is an S3 object for compatibility with AzureStor, while R6 classes are used to represent queues and messages.

```r
library(AzureQstor)

endp <- storage_endpoint("https://mystorage.queue.core.windows.net", key="access_key")

# creating, retrieving and deleting queues
create_storage_queue(endp, "myqueue")
qu <- storage_queue(endp, "myqueue")

qu2 <- create_storage_queue(endp, "myqueue2")
delete_storage_queue(qu2)
```

The queue object exposes methods for getting (reading), peeking, popping (reading and deleting) and putting (writing) messages.

```r
qu$put_message("Hello queue")
msg <- qu$get_message()

msg$text
## [1] "Hello queue"
```

The message object exposes methods for deleting and updating the message.

```r
msg$update(visibility_timeout=30, text="Updated message")
msg$delete()
```
