import csv
import io
import json
import pickle
import struct
import ujson
from functools import partial

import bson
import cbor
import msgpack
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq

from bench_helper import bench_function, baseline_ratio, get_machine_info
from book_pb2 import Book as PBBook
from gendata import get_json_data, get_tuples_data, Book


# [De]Serializers functions

def parquet_dumps(seq):
    df = pd.DataFrame(
        {field: [x[field] for x in seq] for field in range(len(Book._fields))}
    )
    table = pa.Table.from_pandas(df)
    out = io.BytesIO()
    pq.write_table(table, out)
    out.seek(0)
    return out.getvalue()


def parquet_loads(raw):
    # Note: usually want to call to_pandas()
    return pq.read_table(io.BytesIO(raw))


def protobuf_dumps(tuples):
    def to_raw_book(tup):
        book = PBBook()
        book.title = tup[0]
        book.author = tup[1]
        book.sales = tup[2]
        book.is_published = tup[3]

        book.languages.extend(tup[4])

        for review_data in tup[5]:
            review = book.reviews.add()
            review.author = review_data[0]
            review.comment = review_data[1]

        book.price = tup[6]
        return book.SerializeToString()

    raw_books = map(to_raw_book, tuples)
    # encode every message with a prefix of its length
    return b''.join(struct.pack('i', len(raw)) + raw for raw in raw_books)


def protobuf_loads(raw):
    books = []
    len_idx = 0
    while len_idx < len(raw):
        msg_idx = len_idx + 4
        msg_len = struct.unpack('i', raw[len_idx: msg_idx])[0]
        book = PBBook()
        book.ParseFromString(raw[msg_idx: msg_idx + msg_len])
        books.append(book)
        len_idx = msg_idx + msg_len
    return books


SERIALIZERS = (
    ('json', json.dumps, json.loads),
    ('pickle', pickle.dumps, pickle.loads),
    ('bson', lambda seq: b''.join(map(bson.BSON.encode, seq)), bson.decode_all),
    ('ujson', ujson.dumps, ujson.loads),
    ('parquet', parquet_dumps, parquet_loads),
    ('protobuf', protobuf_dumps, protobuf_loads),
    ('cbor', cbor.dumps, cbor.loads),
    ('msgpack', msgpack.dumps, partial(msgpack.loads, raw=False)),
)
BASELINE = 'json'
ITEMS = (1, 1_000, 10_000, 100_000, 1_000_000)
SKIP_LIST = [
    ('dicts', 'parquet'),  # parquet requires schema
    ('dicts', 'protobuf'),  # protobuf requires schema
    ('tuples', 'bson'),  # cannot make bson to work with tuples
]


def main():
    name_tmpl = '{name}_{dtype}_{fn}_{items}'
    results = {}

    print('machine info:')
    print(f'{get_machine_info()}')

    print(f'generating {max(ITEMS)} random data items..', end=' ')
    max_dict_data = get_json_data(n=max(ITEMS))
    max_tuples_data = get_tuples_data(max_dict_data)
    print('done')

    for i, items in enumerate(ITEMS):
        dict_data = max_dict_data[:items]
        tuples_data = max_tuples_data[:items]

        for dtype, data in (('dicts', dict_data), ('tuples', tuples_data)):
            for name, ser_fn, deser_fn in SERIALIZERS:
                print(f'{name} {dtype} {items}..', end=' ')

                # some tests cannot be run (see SKIP_LIST for details)
                if (dtype, name) in SKIP_LIST:
                    print('skip')
                    continue

                # benchmark the format's load & dump
                ser_took, deser_took, ser_size, err = bench_function(name, ser_fn, deser_fn, data)

                # fomat nice name for results
                dump_name = name_tmpl.format(name=name, dtype=dtype, fn='dump', items=items)
                baseline_dump_name = name_tmpl.format(name=BASELINE, dtype=dtype, fn='dump', items=items)
                load_name = name_tmpl.format(name=name, dtype=dtype, fn='load', items=items)
                baseline_load_name = name_tmpl.format(name=BASELINE, dtype=dtype, fn='load', items=items)
                avg_serde = (ser_took.avg + deser_took.avg) / 2.

                common_values = {
                    'err': err,
                    'name': name,
                    'dtype': dtype,
                    'items': items,
                }
                results[dump_name] = {
                    'fn': 'dump',
                    'avg_serde': avg_serde,
                    'serialized_size': ser_size, **common_values,
                    **ser_took._asdict()
                }
                results[load_name] = {
                    'fn': 'load',
                    'avg_serde': avg_serde,
                    'serialized_size': ser_size, **common_values,
                    **deser_took._asdict()
                }

                baseline_ser_took = results[baseline_dump_name]
                baseline_deser_took = results[baseline_load_name]
                results[dump_name].update(
                    baseline_ratio(results[dump_name], baseline_ser_took))
                results[load_name].update(
                    baseline_ratio(results[load_name], baseline_deser_took))
                print('done')

    # write results as detailed json
    with open('detailed-results.json', 'w') as fp:
        json.dump(results, fp, indent=4)

    # write csv results summary
    with open('results-summary.csv', 'w') as fp:
        writer = csv.DictWriter(fp, extrasaction='ignore', fieldnames=(
            'name', 'dtype', 'fn', 'items', 'avg', 'avg_serde',
            'baseline-ratio', 'baseline-speedup', 'serialized_size',
        ))
        writer.writeheader()

        for name, result in sorted(results.items()):
            writer.writerow(result)

    for name, result in sorted(results.items()):
        print(
            f'{name: <40} avg: {result["avg"]:.5f}\t'
            f'baseline-ratio: {result["baseline-ratio"]:.2f}%\t'
            f'speedup: x{result["baseline-speedup"]:.2f}\t'
            f'{result["err"]}'
        )


if __name__ == '__main__':
    main()
