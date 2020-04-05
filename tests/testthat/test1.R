context("Queue client interface")

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

test_that("Queue client interface works",
{
    qu <- stor$get_queue_endpoint()
    qu2 <- queue_endpoint(stor$properties$primaryEndpoints$queue, key=stor$list_keys()[1])
    expect_is(qu, "queue_endpoint")
    expect_identical(qu, qu2)

    expect_true(is_empty(list_storage_queues(qu)))

    # ways of creating a container
    name1 <- make_name()
    sq <- storage_queue(qu, name1)
    create_storage_queue(sq)
    create_storage_queue(qu, make_name())
    create_storage_queue(paste0(qu$url, make_name()), key=qu$key)

    lst <- list_storage_queues(qu)
    expect_true(is.list(lst) && inherits(lst[[1]], "StorageQueue") && length(lst) == 3)

    expect_identical(sq$name, lst[[name1]]$name)

    expect_silent(delete_storage_queue(sq, confirm=FALSE))
})


teardown({
    qu <- stor$get_queue_endpoint()
    lst <- list_storage_queues(qu)
    lapply(lst, delete_storage_queue, confirm=FALSE)
})
