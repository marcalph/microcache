import time
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

N_TASKS = 5
WORK = 10**7  # adjust to see CPU differences


# ----------------------------
# Workloads
# ----------------------------
def cpu_bound(n=WORK):
    return sum(i * i for i in range(n))


def io_bound():
    time.sleep(1)
    return "done"


def hybrid():
    time.sleep(0.5)
    return cpu_bound(WORK // 5)


# ----------------------------
# Sequential
# ----------------------------
def sequential(func):
    start = time.time()
    for _ in range(N_TASKS):
        func()
    return time.time() - start


# ----------------------------
# Threading
# ----------------------------
def threading_run(func):
    """Use ThreadPoolExecutor instead of manual thread management."""
    start = time.time()
    with ThreadPoolExecutor(max_workers=N_TASKS) as executor:
        futures = [executor.submit(func) for _ in range(N_TASKS)]
        # Wait for all futures to complete
        for future in futures:
            future.result()
    return time.time() - start


# ----------------------------
# Multiprocessing
# ----------------------------
def mp_worker(args):
    func, _ = args
    return func()


def mp_run(func):
    """Use ProcessPoolExecutor for cleaner multiprocessing."""
    start = time.time()
    with ProcessPoolExecutor(max_workers=N_TASKS) as executor:
        futures = [executor.submit(mp_worker, (func, i)) for i in range(N_TASKS)]
        # Wait for all futures and handle results
        for future in futures:
            future.result()
    return time.time() - start


# ----------------------------
# Asyncio
# ----------------------------
async def asyncio_worker(func):
    loop = asyncio.get_running_loop()
    if func == io_bound:  # can simulate async sleep
        await asyncio.sleep(1)
        return "done"
    else:
        # offload sync func to thread pool
        return await loop.run_in_executor(None, func)


async def asyncio_run(func):
    """Use asyncio.gather for cleaner task management."""
    start = time.time()
    # Create and gather all tasks in one statement
    results = await asyncio.gather(
        *[asyncio_worker(func) for _ in range(N_TASKS)], return_exceptions=True
    )
    # Handle any exceptions that occurred
    for result in results:
        if isinstance(result, Exception):
            raise result
    return time.time() - start


# ----------------------------
# Thread + Async (async coros dispatch work to thread pool)
# ----------------------------
async def async_thread(func):
    """Properly sized ThreadPoolExecutor with asyncio."""
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=N_TASKS) as pool:
        # Use gather with executor futures
        results = await asyncio.gather(
            *[loop.run_in_executor(pool, func) for _ in range(N_TASKS)],
            return_exceptions=True,
        )
        # Handle any exceptions
        for result in results:
            if isinstance(result, Exception):
                raise result


def thread_async_run(func):
    start = time.time()
    asyncio.run(async_thread(func))
    return time.time() - start


# ----------------------------
# MP + Async (async dispatch to process pool)
# ----------------------------
async def async_mp(func):
    """Properly sized ProcessPoolExecutor with asyncio."""
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(max_workers=N_TASKS) as pool:
        # Use gather with executor futures
        results = await asyncio.gather(
            *[loop.run_in_executor(pool, func) for _ in range(N_TASKS)],
            return_exceptions=True,
        )
        # Handle any exceptions
        for result in results:
            if isinstance(result, Exception):
                raise result


def mp_async_run(func):
    start = time.time()
    asyncio.run(async_mp(func))
    return time.time() - start


# ----------------------------
# Benchmark runner
# ----------------------------
def benchmark(func, label):
    """Run benchmarks with proper error handling."""
    print(f"\n=== {label} ===")

    results = {}
    benchmarks = [
        ("Sequential", lambda: sequential(func)),
        ("Threading", lambda: threading_run(func)),
        ("Asyncio", lambda: asyncio.run(asyncio_run(func))),
        ("Multiproc", lambda: mp_run(func)),
        ("Thread+Async", lambda: thread_async_run(func)),
        ("MP+Async", lambda: mp_async_run(func)),
    ]

    for name, bench_func in benchmarks:
        try:
            result = bench_func()
            results[name] = result
            print(f"{name:12}: {result:.4f}s")
        except Exception as e:
            print(f"{name:12}: Failed - {e}")
            results[name] = None

    # Find and display the fastest method
    valid_results = {k: v for k, v in results.items() if v is not None}
    if valid_results:
        fastest = min(valid_results.items(), key=lambda x: x[1])
        print(f"\n🏆 Fastest: {fastest[0]} ({fastest[1]:.4f}s)")


if __name__ == "__main__":
    print("⚡ I/O bound test")
    benchmark(io_bound, "I/O-bound")

    print("\n🧮 CPU bound test")
    benchmark(cpu_bound, "CPU-bound")

    print("\n🔀 Hybrid test")
    benchmark(hybrid, "Hybrid")
