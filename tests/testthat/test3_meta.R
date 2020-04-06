context("Metadata")

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

test_that("Metadata set/get works",
{
    sq <- create_storage_queue(qu, make_name())
    expect_is(sq, "StorageQueue")

    meta <- sq$get_metadata()
    expect_true(length(meta) == 0)

    meta2 <- get_storage_metadata(sq)
    expect_true(length(meta2) == 0)

    set_storage_metadata(sq, name1="value1")
    meta3 <- get_storage_metadata(sq)
    expect_identical(meta3, list(name1="value1"))

    sq$set_metadata(name2="value2")
    meta4 <- sq$get_metadata()
    expect_identical(meta4, list(name1="value1", name2="value2"))

    set_storage_metadata(sq, name3="value3", keep_existing=FALSE)
    meta5 <- get_storage_metadata(sq)
    expect_identical(meta5, list(name3="value3"))
})


teardown({
    lst <- list_storage_queues(qu)
    lapply(lst, delete_storage_queue, confirm=FALSE)
})
