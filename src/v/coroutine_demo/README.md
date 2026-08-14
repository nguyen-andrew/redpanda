# A guided tour: coroutine lifetimes and exceptions in Seastar

This directory is a self-contained lesson. It answers two questions that
sound simple and are not:

1. **When do objects inside a coroutine get destroyed?** (It depends on
   whether the object is a local variable or a parameter, and the
   difference is large.)
2. **Where does an exception "live" while it travels through async
   code?** (Sometimes it is a real, in-flight C++ exception. Most of the
   time it is just a value stored in a future. Code that assumes one when
   the other is true can crash the process.)

By the end you will be able to look at a coroutine that takes an object
by value, ask "what happens to that object if the body throws?", and
answer precisely. That question is not academic: a production Redpanda
crash came down to exactly it, and the last section of this tour shows
you that bug shape.

**What you need coming in:** you know that Seastar code returns
`ss::future<>`, that a coroutine is a function using `co_await`
/`co_return`, and that awaiting a future gives up the CPU until the
future resolves. Nothing else is assumed. Every claim in this tour is
demonstrated by a program you can run, and the transcripts quoted below
are the committed golden files that `run_all.sh` re-verifies (a few
lines carry added `<-` annotations).

## Running the tour

```bash
# run everything and verify the transcripts:
src/v/coroutine_demo/run_all.sh

# or run one scenario at a time as you read:
bazel build //src/v/coroutine_demo:lifetime_windows_demo //src/v/coroutine_demo:exception_flow_demo
bazel-bin/src/v/coroutine_demo/lifetime_windows_demo --scenario suspension-not-scope-exit
```

Transcript lines start with `|`. Headers like `==== ... ====` are
printed by the scenario code to group what follows; anything without the
`|` prefix is Seastar startup noise.

**The one prop used everywhere:** `demo::instrumented` (in
`instrumented.h`) is a class that prints a line from each of its special
member functions, stamped with the object's label and the member that is
running, so `param#2.~instrumented()` means "the destructor of the
object labeled param#2 is executing right now". Each move-construction
bumps the label's generation (`param` begets `param#1` begets
`param#2`), so every distinct object is distinguishable. The destructor
also prints the only two things any destructor can consult to ask "is an
exception in flight right now?":

- `std::uncaught_exceptions()`: how many exceptions are currently
  unwinding the stack (destroying objects on their way to a catch).
- `std::current_exception()`: non-null only while inside a catch block.

Keep an eye on those two values in every transcript. The entire lesson
is about moments where they tell the truth and one moment where they
cannot.

---

## Part 1: a coroutine is a heap object that runs in bursts

Before touching exceptions, we need an accurate picture of what a
coroutine *is*. Two facts replace the intuition of "a function that can
pause":

**Fact 1: the body starts running immediately at the call** (Seastar
coroutines are "eager"), and it runs until the first `co_await` that
actually has to wait. At that point the call returns to the caller with a
not-yet-ready future. The coroutine's variables live on in a compiler-
generated heap allocation called the **frame**. Later, when the awaited
future resolves, the reactor **resumes** the coroutine: runs the next
burst of the body, until the next suspension or the end.

**Fact 2: suspending destroys nothing.** A local variable that is in
scope at a suspension point simply stays alive in the frame, across any
amount of time and any number of other tasks running.

Run `--scenario suspension-not-scope-exit`:

```
| ==== call the coroutine; it runs eagerly until its first suspension ====
|
| local.instrumented()           constructed
| coro: suspending; the local stays alive
|
| ==== suspended: the caller runs while the local stays alive ====
|
| caller: I have control and no destructor has run
| coro: resumed; the local is still alive
| local.~instrumented()          uncaught_exceptions=0 current_exception=null
| caller: coroutine finished
```

Read it top to bottom: the coroutine ran eagerly (its lines print before
the caller's), the caller got control back at the suspension with the
local still alive, and the destructor ran only when the body finished
and the local went out of scope. Suspension is not scope exit.

### Parameters are copied into the frame, and there are more objects than you think

Now the detail this whole tour turns on. When a coroutine takes a
parameter **by value**, the language requires the coroutine to copy that
parameter into its frame, because the frame outlives the call
expression. So the object the body uses is not the object the caller
passed.

Run `--scenario no-suspension`, which calls a coroutine that finishes
without ever suspending:

```
| ==== call a coroutine that never suspends: its whole life happens inside the call ====
|
| param.instrumented()           constructed
| param#1.instrumented(&&)       move-constructed from param; param is now moved-from
| param#2.instrumented(&&)       move-constructed from param#1; param#1 is now moved-from
| param#2.note_alive()           the coroutine body uses this object
| coro: completing without ever suspending
| param#2.~instrumented()        uncaught_exceptions=0 current_exception=null
| param#1.~instrumented()        [moved-from] uncaught_exceptions=0 current_exception=null
|
| ==== back in the caller: the frame and its copy are already gone ====
|
| param.~instrumented()          [moved-from] uncaught_exceptions=0 current_exception=null
```

Three objects exist:

- `param`: the named variable in the caller. After `std::move(param)` it
  is an empty shell, destroyed at the end of the caller's scope (last
  line).
- `param#1`: the by-value function parameter itself, materialized by the
  caller for the call. On our platform the caller also destroys it, at
  the end of the statement containing the call.
- `param#2`: the copy the coroutine made **in its frame**. This is the
  only one the body ever touches (`param#2.note_alive()`), and,
  critically, the only one destroyed by **frame teardown**, the
  compiler-generated cleanup that destroys the frame's contents when the
  coroutine finishes.

Also notice *where* `param#2.~instrumented()` prints: before the "back
in the caller" header. This coroutine never suspended, so its entire
life, teardown included, happened inside the call itself. Tuck that
away; it becomes the escape hatch in Part 4.

> **Takeaway from Part 1.** A coroutine's locals follow normal scope
> rules, just stretched across suspensions. A coroutine's by-value
> parameter is a distinct copy that lives in the frame and is destroyed
> by frame teardown, not by scope exit. "Frame teardown" happens when
> the body finishes, wherever that is.

---

## Part 2: what a destructor can know about exceptions (no coroutines yet)

Forget coroutines for a moment. In plain C++, a destructor can run in
three different situations with respect to exceptions, and it can try to
tell them apart using the two introspection calls from the intro. Run
`--scenario windows`:

```
| ==== baseline: plain scope exit, no exception anywhere ====
|
| baseline-local.instrumented()  constructed
| baseline-local.~instrumented() uncaught_exceptions=0 current_exception=null
|
| ==== window 1: destroyed while an exception unwinds the scope ====
|
| w1-local.instrumented()        constructed
| w1-local.~instrumented()       uncaught_exceptions=1 current_exception=null
|
| ==== window 2: destroyed inside the catch handler ====
|
| w2-local.instrumented()        constructed
| w2-local.~instrumented()       uncaught_exceptions=0 current_exception=non-null
|
| ==== window 3: destroyed after the handler exited ====
|
| w3-local.instrumented()        constructed
| w3-local.~instrumented()       uncaught_exceptions=0 current_exception=null
| note: window 3 reads exactly like the baseline
```

| destroyed... | `uncaught_exceptions()` | `current_exception()` |
|---|---|---|
| baseline: no exception anywhere | 0 | null |
| window 1: while unwinding toward a catch | 1 | null |
| window 2: inside the catch block | 0 | non-null |
| window 3: after the catch finished | 0 | null |

Look at the first and last rows: **identical**. Once an exception has
been caught and handled, the world reads exactly like a world where
nothing ever went wrong. That is fine and correct for normal code. It
becomes a trap the moment some destructor *needs* to know "did an error
already happen?", runs in window 3, and concludes "no".

Why would a destructor need to know that? A real pattern in this
codebase: a type whose destructor enforces an obligation. "If nobody
checked authorization before I died, and nothing else already went
wrong, throw an error so the request fails loudly instead of leaking
data." Such a destructor throws only when both introspection calls read
clear. Keep the windows table in mind; we are about to drop this exact
pattern into a coroutine.

> **Takeaway from Part 2.** "Is an exception in flight?" is answerable
> during unwinding (window 1) and inside a catch (window 2). After the
> catch, the exception is invisible (window 3). A destructor running in
> window 3 cannot distinguish "an error was already handled" from "all
> is well".

---

## Part 3: a coroutine fails; where do the local and the parameter die?

Now combine the two parts. What does "throwing an exception" even mean
in a coroutine, where the caller is long gone and there is no stack
connecting them? Here is the machinery, in the order it acts. The
compiler wraps every coroutine body roughly like this (simplified):

```
frame = { parameter copies, suspension-crossing locals, the promise }

try {
    ... your body, with its locals ...
} catch (...) {
    // the implicit catch-all: nothing escapes a coroutine as a throw.
    // The exception is captured and STORED into the coroutine's result
    // future, to be delivered to whoever awaits it.
    store current exception into the result future
}
// body finished, one way or the other:
frame teardown: destroy parameter copies and the promise, free the frame
```

So when a coroutine body fails after a real suspension, one resume of
that coroutine performs this sequence:

```
1. the awaited future turns out to be failed
   -> co_await RETHROWS the stored exception as a live throw
2. unwinding destroys the locals in scope         <- window 1
3. the implicit catch-all catches it and stores it
   into the result future                          <- exception becomes data
4. frame teardown destroys the parameter copies    <- window 3 (nothing visible!)
5. the resume returns; the caller will be woken and see the failed future
```

Steps 2 and 4 are the punchline: **the local dies during unwinding,
where the exception is visible; the parameter dies in teardown, after
the catch-all, where it is not.** The parameter is never "unwound" at
all, because it is not scope state, it is frame state.

Run `--scenario local-vs-param`: one coroutine holding the same
instrumented type both as a local and as a by-value parameter, failing
after a real suspension.

```
| ==== the caller builds the argument and calls the coroutine ====
|
| param.instrumented()           constructed
| param#1.instrumented(&&)       move-constructed from param; param is now moved-from
| param#2.instrumented(&&)       move-constructed from param#1; param#1 is now moved-from
| param#2.note_alive()           the coroutine body uses this object
| local.instrumented()           constructed
| coro: suspending on a future that will fail
|
| ==== the failure arrives after a real suspension ====
|
| helper: resumed; throwing
| local.~instrumented()          uncaught_exceptions=1 current_exception=null    <- window 1
| param#2.~instrumented()        uncaught_exceptions=0 current_exception=null    <- window 3
| param#1.~instrumented()        [moved-from] uncaught_exceptions=0 current_exception=null
|
| ==== the caller finds out ====
|
| caller: coroutine failed, received as data: failed=true
| caller: the local unwound; the param was destroyed in the blind window
| param.~instrumented()          [moved-from] uncaught_exceptions=0 current_exception=null
```

Walk the important lines:

- `local.~instrumented()` reports `uncaught_exceptions=1`: step 2. The
  local sees window 1, a real unwind. If this destructor needed to know
  "did something go wrong?", it would learn the truth.
- `param#2.~instrumented()` reports both values clear: step 4. The frame
  copy of the parameter is destroyed moments later, in the same resume,
  but the exception has already been converted into data inside the
  result future. This is window 3, and note it happens *before* the
  caller has even been told (the caller's lines print afterward). At
  this instant, the exception exists nowhere on any stack; it is a value
  in a future.
- The two `[moved-from]` shells from Part 1 are destroyed on their
  ordinary schedules and see nothing unusual.

> **Takeaway from Part 3.** In a failing coroutine, locals are destroyed
> above the implicit catch-all (exception visible), parameters below it
> (exception invisible). Between those two moments the exception stops
> being a throw and becomes data.

---

## Part 4: give that destructor a reason to throw

Part 2 introduced the obligation-enforcing destructor: throw if nobody
checked me and nothing else already went wrong. `instrumented` has a
mode that models it (`throw_when_clear`): at destruction, if both
introspection calls read clear, it throws. Moving from an `instrumented`
object marks the source as moved-from and disarms it, the way a real
obligation transfers to wherever the object moved.

We now know such an object inside a coroutine can die in two places.
Both scenarios below fail the same way; only the placement differs.

**As a local** (`--scenario throwing-dtor-local`, exit code 0):

```
| ==== a guard object as a coroutine LOCAL: its throw becomes a failed future ====
|
| local.instrumented()           constructed (mode: throw_when_clear)
| coro: local goes out of scope now, with nothing in flight
| local.~instrumented()          uncaught_exceptions=0 current_exception=null
| local.~instrumented()          no exception visible here; throwing
| caller: caught the dtor throw as a failed future: thrown by ~local
| caller: the process is fine
```

The local's throw happens inside the body's territory, so the implicit
catch-all does its job: the throw is captured, stored into the result
future, and the caller receives it like any other error. The design
intent of the throwing destructor survives coroutines *when the object
is a local*.

**As a parameter** (`--scenario throwing-dtor-param`, exit code 134):

```
| ==== a guard object as a by-value PARAMETER: expect std::terminate ====
|
| param.instrumented()           constructed (mode: throw_when_clear)
| param#1.instrumented(&&)       move-constructed from param; param is now moved-from
| param#2.instrumented(&&)       move-constructed from param#1; param#1 is now moved-from
| param#2.note_alive()           the coroutine body uses this object
| coro: suspending on a future that will fail
|
| ==== the failure arrives after a real suspension ====
|
| helper: resumed; throwing
| param#2.~instrumented()        uncaught_exceptions=0 current_exception=null    <- window 3
| param#2.~instrumented()        no exception visible here; throwing
libc++abi: terminating due to uncaught exception of type std::runtime_error: thrown by ~param#2
```

(The last line is the raw abort message, shown for context; the golden
files contain only the `|` lines.)

Follow it with the Part 3 sequence in hand. The body failed after a real
suspension; the exception was already stored as data (that is why
`param#2`'s destructor reads all-clear); and then the destructor throws,
below the catch-all. What is above it to catch the throw? Only the
reactor's task loop. Resuming a coroutine happens inside
`seastar::task::run_and_dispose()`, and that function is declared
`noexcept`. A throw meeting a `noexcept` boundary is `std::terminate()`.
The process dies with SIGABRT (exit code 134). The symbolized backtrace
of this demo's abort (fastbuild binary, addr2line) is the fingerprint to
look for in real crash reports:

```
std::terminate()
__clang_call_terminate
seastar::internal::coroutine_traits_base<void>::promise_type::run_and_dispose()
seastar::reactor::task_queue::run_tasks()
```

Two cruelties make this bug nasty in real life. First, the destructor's
decision was *wrong*: an error had already happened and an error
response was already on its way to the caller; the guard threw its
"nobody handled the error!" alarm precisely because it is blind in
window 3. Second, the `noexcept` is not the villain: by the time
teardown runs, the result future has already been fulfilled (a future
delivers exactly one result), so even without the `noexcept` there would
be nobody left to deliver the throw to.

**The boundary of the fatal shape**
(`--scenario throwing-dtor-param-no-suspension`, exit code 0): recall
from Part 1 that a coroutine which fails *before its first real
suspension* runs its whole life, teardown included, inside the original
call. There, an ordinary try/catch around the call still surrounds the
teardown, and the destructor's throw is absorbed like any other:

```
| ==== the same guard parameter, but the body throws before any suspension ====
|
| param.instrumented()           constructed (mode: throw_when_clear)
| param#1.instrumented(&&)       move-constructed from param; param is now moved-from
| param#2.instrumented(&&)       move-constructed from param#1; param#1 is now moved-from
| param#2.note_alive()           the coroutine body uses this object
| coro: throwing before any suspension
| param#2.~instrumented()        uncaught_exceptions=0 current_exception=null
| param#2.~instrumented()        no exception visible here; throwing
| param#1.~instrumented()        [moved-from] uncaught_exceptions=1 current_exception=null
| param.~instrumented()          [moved-from] uncaught_exceptions=1 current_exception=null
| caller: caught during the call itself: thrown by ~param#2
| caller: a plain try/catch absorbed it; the process is fine
```

(As a bonus, the two moved-from shells are destroyed by that unwind and
correctly report window 1.) So the guaranteed-fatal recipe is: by-value
parameter + throwing destructor + body failure **after at least one real
suspension**, because after a suspension every resume runs from the
reactor's `noexcept` task loop.

> **Takeaway from Part 4.** A destructor that signals by throwing is
> incompatible with being (or being owned by) a coroutine parameter.
> The recoverable variant of the same destructor is the frame LOCAL,
> which is why "move the parameter into a local at the top of the body"
> is a real fix shape, and why "make the destructor never throw and
> enforce the obligation elsewhere" is the durable one.

---

## Part 5: how an exception actually travels through async code

Part 3 showed one coroutine converting a throw into data. This part
shows the pattern at scale, because it explains both a correctness
intuition and a performance rule you will meet in review comments.

The measuring device: `demo::unwind_events()` counts instrumented
destructions that ran during unwinding (window 1). Every unwind event
means one live throw passed through that frame. All three scenarios
share the same failure source, a coroutine `origin_fails()` whose own
local contributes exactly one unavoidable unwind event at the throw
site. These scenarios are in the second binary:

```bash
bazel-bin/src/v/coroutine_demo/exception_flow_demo --scenario per-layer-rethrow
```

**Through three nested coroutines** (`per-layer-rethrow`), each layer
just `co_await`ing the next:

```
| ==== build four nested coroutines; they run eagerly down to the origin and suspend ====
|
| layer3-local.instrumented()    constructed
| layer2-local.instrumented()    constructed
| layer1-local.instrumented()    constructed
| origin-local.instrumented()    constructed
|
| ==== the origin resumes and throws ====
|
| origin-local.~instrumented()   uncaught_exceptions=1 current_exception=null
| layer1-local.~instrumented()   uncaught_exceptions=1 current_exception=null
| layer2-local.~instrumented()   uncaught_exceptions=1 current_exception=null
| layer3-local.~instrumented()   uncaught_exceptions=1 current_exception=null
|
| ==== what the caller saw ====
|
| caller: chain failed, received as data: failed=true
| unwind events observed: 4 (origin plus one per coroutine layer)
```

Four unwind events for one failure. Each layer's `co_await` rethrew the
exception as a live throw inside that layer's frame (that is the only
way `co_await` can deliver a failure to a coroutine body), its implicit
catch-all re-stored it as data, and the cycle repeated one layer up.
There is no single grand unwind across the async call chain, because no
real stack connects the layers; there is a small, complete
throw/unwind/catch cycle inside every layer, typically one reactor task
each.

**Through a `.then()` continuation chain** (`then-chain`), same failure:

```
| ==== the same failure through a .then() chain: continuations are skipped ====
|
| origin-local.instrumented()    constructed
|
| ==== the origin resumes and throws ====
|
| origin-local.~instrumented()   uncaught_exceptions=1 current_exception=null
| consumer: received failed=true as data; no continuation ran
| unwind events observed: 1 (origin only; forwarding added none)
```

One unwind event, at the origin. The three `.then()` continuations in
the chain printed nothing: a failed future skips the continuation
entirely and moves the stored exception into the next future as data.
No rethrow, no unwinding, anywhere in the forwarding path.

**Consumed with `ss::coroutine::as_future`** (`as-future`): a coroutine
can opt out of the rethrow too. Awaiting
`ss::coroutine::as_future(fut)` hands the failed future over *as a
value* to inspect:

```
| ==== the same failure consumed as a value with ss::coroutine::as_future ====
|
| consumer-local.instrumented()  constructed
| origin-local.instrumented()    constructed
|
| ==== the origin resumes and throws ====
|
| origin-local.~instrumented()   uncaught_exceptions=1 current_exception=null
| consumer: received failed=true as data; nothing was rethrown here
| unwind events observed: 1 (origin only; this layer stayed on the value path)
| consumer-local.~instrumented() uncaught_exceptions=0 current_exception=null
```

The consumer's own local is destroyed on the normal path afterward: no
throw ever entered this frame.

> **Takeaway from Part 5.** An exception in async code is data at rest
> (an `exception_ptr` inside a future) and becomes a live throw only at
> a `co_await` of a failed future, once per coroutine layer. `.then()`
> chains and `as_future` keep it as data. This is also the performance
> story behind the house guidance to prefer `ss::coroutine::as_future`
> and friends on hot paths: deep coroutine chains pay a throw/catch
> cycle per layer for every failure.

---

## The bug you can now read

Here is the production incident shape, stated plainly. Check yourself:
every sentence should now feel obvious.

Redpanda's HTTP layers have an authorization-result type whose
destructor enforces an obligation: if the request handler never called
any of the "check permissions" methods on it, and the introspection
calls say nothing else went wrong, the destructor throws, turning the
forgotten check into an error response instead of silently serving
data. The design predates coroutines, and in the pre-coroutine world it
was sound: every place the destructor could run, the throw either
unwound into the HTTP framework's catch (which maps it to an error
response) or was captured into a future by continuation machinery.

Then one handler path started receiving that object as a **by-value
coroutine parameter**. The handler performs its authorization check
partway through the body, after some `co_await`s. One day, an earlier
`co_await` threw (a storage read failed), so the body never reached the
check:

- The read's exception unwound the handler's locals and was stored by
  the implicit catch-all; an error response was on its way. (Part 3.)
- Frame teardown then destroyed the parameter copy: unchecked, and in
  window 3, where the already-handled exception is invisible. (Parts 2
  and 3.)
- The guard concluded "nobody checked me and nothing went wrong",
  logged, and threw, below the catch-all, inside the reactor's
  `noexcept` task loop. (Part 4.)
- `std::terminate`, SIGABRT, one crashed broker per unlucky request,
  with `__clang_call_terminate` over
  `...promise_type::run_and_dispose()` in the backtrace, and the guard's
  own error line as the last log entry.

And the fixes should feel obvious too, in both directions shown in Part
4: at the call sites, keep the obligation in a frame local (or complete
the check before the object can be destroyed unchecked); in the type,
never throw from a destructor that can run in cleanup contexts, and
enforce the obligation at a point that still has a channel back to the
client.

## Where the machinery lives (for the skeptical reader)

Everything above is fixed by a handful of places in the vendored
seastar, paths under `external/+non_module_dependencies+seastar/`:

- `include/seastar/core/coroutine.hh`: `initial_suspend` and
  `final_suspend` both return `suspend_never` (eager start; the frame
  destroys itself at completion, which is when teardown runs);
  `unhandled_exception()` stores `std::current_exception()` into the
  result future (the implicit catch-all's storage step); the awaiter's
  `await_resume()` calls `future::get()`, which is the rethrow at
  `co_await`.
- `include/seastar/core/task.hh`: `virtual void run_and_dispose()
  noexcept`. The coroutine's promise IS a `seastar::task`, and its
  `run_and_dispose()` just resumes the coroutine: every post-suspension
  burst of a coroutine body, and its teardown, runs under this
  `noexcept`.
- `include/seastar/core/future.hh`: `then_impl`'s failed-future fast
  path: a failed future short-circuits past the continuation and into
  `make_exception_future`, the zero-rethrow forwarding of Part 5.
- `include/seastar/coroutine/as_future.hh`: the awaiter that hands a
  failed future over as a value.

One honest limitation: the implicit catch-all is compiler-generated, so
no print statement can run inside it. The evidence that it ran is
indirect but tight: the parameter's destructor reads all-clear (so the
exception is no longer unwinding), yet the caller still receives that
same exception afterward, so between those two lines it must have been
stored.

A note on method: the golden transcripts were written as *predictions*
from this mental model before the programs first ran. Six of ten matched
exactly; the miss was instructive and consistent. The model said "a
by-value argument means two objects"; every parameter scenario printed a
third (`param#1`, the parameter object itself, with caller-side
destruction timing). The goldens were corrected to match reality, which
is the point of the exercise: the transcripts you see are what the real
toolchain does, not what the model hoped.
