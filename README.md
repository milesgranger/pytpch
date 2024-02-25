Ergonomically create [TPC-H](https://www.tpc.org/tpch/) data thru Python as Arrow tables.


**NOTE**:
    This was a weekend project, it is a WIP. For now only x86_64 linux wheels are available on PyPI

```python

import pytpch
import pyarrow as pa

# Generate TPC-H data at scale 1 (~1GB)
tables: dict[str, pa.Table] = pytpch.dbgen(sf=1)

# Generate a single table at scale 1
tables: dict[str, pa.Table] = pytpch.dbgen(sf=1, table=pytpch.Table.Nation)

# Generate a single chunk out of n chunks of a single table
# this is wildly helpful when generating larger scale factors as you can make
# subsets of the data and store them or join them after some sort of parallelism.
tables: dict[str, pa.Table] = pytpch.dbgen(sf=1, n_steps=10, step=1, table=pytpch.Table.Nation)


# NOTE! As mentioned in the docs for this function, it is NOT thread-safe.
#       If you want to generate data in parallel, you must do so in other processes for now
#       by using things like `multiprocessing` or `concurrent.futures.ProcessPoolExecutor`.
#       This is a TODO, as the original C code uses copious amounts of global and static function
#       variables to maintain state, and while the state is reset between function calls from refactoring
#       in milesgranger/libdbgen, these shared global states are not removed so thus not thread-safe.
#
# Example of generating data in parallel:
from concurrent.futures import ProcessPoolExecutor

n_steps = 10  # 10 total chunks

def gen_step(step):
    return pytpch.dbgen(sf=10, n_steps=n_steps, nth_step=step)

with ProcessPoolExecutor() as executor:
    jobs: list[dict[str, pa.Table]] = list(executor.map(gen_step, range(n_steps)))
  

# Default reference queries provided (1-22) as:
print(pytpch.QUERY_1)
```

---

### Tell me more...

Python bindings (thru Rust, b/c why not) to [libdbgen](https://github.com/milesgranger/libdbgen) 
which is a fork of [databricks/tpch-dbgen](https://github.com/databricks/tpch-dbgen) for generating 
[TPC-H data](https://www.tpc.org/tpch/).

tpch-dbgen is originally a CLI to generate CSV files for TPC-H data. I wanted to make it into an ergonomic
Python API for use in other projects. 

TODOS (roughly in order of priority):
  - [ ] Support for more than Linux x86_64 (mostly just adapting C lib and updating CI)
  - [ ] Remove verbose stdout
  - [ ] Write directly to Arrow, removing CSV writing (w/ nanoarrow probably)
  - [ ] Make thread safe (remove global and static function variables in C lib, and remove changing of CWD)
  - [ ] Separate out the Rust stuff into it's own crate.

### Build from source...

Roughly:

- `git clone --recursive git@github.com:milesgranger/pytpch.git`
- `python -m pip install maturin`
- `maturin build --release`

That'll only work if you're on x86_64 linux for now, you can try adapting `build.rs` but good luck with that. For now.
