# Optimized Query Processing

In normal Raft, any application commands are appended to log and brought to the state machine to be applied. However, if the command is query type which doesn't have any side effects, there is a possible optimization.

In `RequestApply`, if the request's `mutation` is false then the request is recognized as query. As described in $6.4 of Raft dissertation, the query waits for the `read_index` (the `commit_index` at the moment the query hits the server) to be applied.

In the implementation, queries are firstly put into `QueryQueue` and processed later.
The processings are done in parallel if there are more than one queries waiting for the same `read_index` to complete.