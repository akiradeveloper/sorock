# Orbit Pattern

Inside RaftProcess, there is a state called `RaftCore` and
many threads to process it concurrently on an event-driven basis,
most of them are driven by timers.
Since this is like threads are orbiting around the core,
I call this **Orbit Pattern**.

![](images/orbit-pattern.png)