import time, asyncio, threading, multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

N_TASKS = 5
WORK = 10**7  # adjust to see CPU differences


# ----------------------------
# Workloads
# ----------------------------
def cpu_bound(n=WORK):
    return sum(i*i for i in range(n))

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
    start = time.time()
    threads = [threading.Thread(target=func) for _ in range(N_TASKS)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    return time.time() - start


# ----------------------------
# Multiprocessing
# ----------------------------
def mp_worker(args):
    func, _ = args
    return func()

def mp_run(func):
    start = time.time()
    with multiprocessing.Pool(N_TASKS) as pool:
        pool.map(mp_worker, [(func, i) for i in range(N_TASKS)])
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
    start = time.time()
    tasks = [asyncio.create_task(asyncio_worker(func)) for _ in range(N_TASKS)]
    await asyncio.gather(*tasks)
    return time.time() - start


# ----------------------------
# Thread + Async (async coros dispatch work to thread pool)
# ----------------------------
async def async_thread(func):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        futures = [loop.run_in_executor(pool, func) for _ in range(N_TASKS)]
        await asyncio.gather(*futures)

def thread_async_run(func):
    start = time.time()
    asyncio.run(async_thread(func))
    return time.time() - start


# ----------------------------
# MP + Async (async dispatch to process pool)
# ----------------------------
async def async_mp(func):
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor() as pool:
        futures = [loop.run_in_executor(pool, func) for _ in range(N_TASKS)]
        await asyncio.gather(*futures)

def mp_async_run(func):
    start = time.time()
    asyncio.run(async_mp(func))
    return time.time() - start


# ----------------------------
# Benchmark runner
# ----------------------------
def benchmark(func, label):
    print(f"\n=== {label} ===")
    print("Sequential:", sequential(func))
    print("Threading :", threading_run(func))
    print("Asyncio   :", asyncio.run(asyncio_run(func)))
    print("Multiproc :", mp_run(func))
    print("Thread+Async:", thread_async_run(func))
    print("MP+Async    :", mp_async_run(func))


if __name__ == "__main__":
    print("⚡ I/O bound test")
    benchmark(io_bound, "I/O-bound")

    print("\n🧮 CPU bound test")
    benchmark(cpu_bound, "CPU-bound")

    print("\n🔀 Hybrid test")
    benchmark(hybrid, "Hybrid")
