context("Queue functionality")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_STORAGE_RG")
storname <- Sys.getenv("AZ_TEST_STORAGE_HNS")

if(rgname == "" || storname == "")
    skip("Queue client tests skipped: resource names not set")

sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)
stor <- sub$get_resource_group(rgname)$get_storage_account(storname)
options(azure_storage_progress_bar=FALSE)

qu <- stor$get_queue_endpoint()

test_that("Queueing and messaging works",
{
    sq <- create_storage_queue(qu, make_name())
    expect_is(sq, "StorageQueue")

    expect_silent(sq$put_message(text="message 1"))
    msg <- sq$get_message()
    expect_is(msg, "QueueMessage")
    expect_identical(msg$text, "message 1")

    expect_silent(msg$delete())
    msg <- sq$get_message()
    expect_null(msg$text)

    sq$put_message(text="message 2")
    msg2 <- sq$peek_message()
    expect_identical(msg2$text, "message 2")

    msg3 <- sq$peek_message()
    expect_identical(msg3$text, "message 2")

    expect_error(msg3$delete())
    expect_error(msg3$update(30))

    msg4 <- sq$pop_message()
    expect_error(msg4$delete())

    sq$put_message("message 3")
    msg5 <- sq$get_message()
    expect_silent(msg5$update(1, text="new text"))
    expect_identical(msg5$text, "new text")

    Sys.sleep(3)
    msg6 <- sq$peek_message()
    expect_identical(msg6$text, "new text")
    expect_silent(msg5$delete())
})


teardown({
    lst <- list_storage_queues(qu)
    lapply(lst, delete_storage_queue, confirm=FALSE)
})
