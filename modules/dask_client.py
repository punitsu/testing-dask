from dask.distributed import Client
import multiprocessing

def client():
    n_workers = multiprocessing.cpu_count()
    threads_per_worker = 2
    client = Client(n_workers=n_workers, threads_per_worker=threads_per_worker)
    return client