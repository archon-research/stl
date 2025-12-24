look into deduplication
look into backfilling
look into reorg handling
handle unrecoverable panics somehow
    maybe in code
    maybe in infra
tests
metrics (prometheus / opentelemetry)
data sink
sql in separate files

What happens if 10 workers compete for receipts in a sqs queue? Race condition?
    Need some kind of versioning on a piece of data
    so block needs to be unique by block number, hash, chain, version (v1,v2,v3)
    or we only have one worker per sqs queue for now
    
watcher has to write cache entries

what happens if the watcher fails to publish? 
    Just backfill?

What happens if a reorg happens right when the connection is killed?
    is there a race condition?

With the gap based backfilling, how is query performance on a large table?
    If we do pruning on old rows, should it need to make sure there is no gaps before pruning?