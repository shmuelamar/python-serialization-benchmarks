import platform
import statistics
import time
from collections import namedtuple

Took = namedtuple('Took', ['avg', 'min', 'max', 'stdev', 'timings'])


def bench_function(name, serialize_fn, deserialize_fn, data):
    raw, ser_took = took(serialize_fn, repeats=10, args=(data,))
    des_data, deser_took = took(deserialize_fn, repeats=10, args=(raw,))
    err_msg = ''

    assert len(data) == len(des_data)
    return ser_took, deser_took, len(raw), err_msg


def get_machine_info():
    system = platform.system()
    bits = platform.architecture()[0]
    pyver = f'{platform.python_implementation()} {platform.python_version()}'
    py_build_ver = f'{platform.python_compiler()} {"-".join(platform.python_build())}'  # noqa
    return f'{system} {bits}, {pyver} build: {py_build_ver}'


def took(fn, repeats=1, args=(), ndigits=6):
    timings = []
    res = None

    for _ in range(repeats):
        runtime = -time.process_time()
        res = fn(*args)

        runtime += time.process_time()
        timings.append(runtime)

    min_time = min(timings)
    max_time = max(timings)
    avg_time = sum(timings) / repeats
    stdev_time = statistics.pstdev(timings)

    return res, Took(
        avg=round(avg_time, ndigits),
        min=round(min_time, ndigits),
        max=round(max_time, ndigits),
        stdev=round(stdev_time, ndigits),
        timings=[round(t, 5) for t in timings],
    )


def baseline_ratio(fn_took, baseline_took, ndigits=4):
    return {
        'baseline-speedup': round(baseline_took['avg'] / fn_took['avg'], ndigits),
        'baseline-ratio': round(fn_took['avg'] * 100 / baseline_took['avg'], ndigits),
    }
