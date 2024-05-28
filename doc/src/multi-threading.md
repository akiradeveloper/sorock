# Orbit Pattern

Inside RaftProcess, there is a state called `RaftCore` and
many threads to process it concurrently on an event-driven basis,
most of them are driven by timers.

![](images/multi-threading.png)