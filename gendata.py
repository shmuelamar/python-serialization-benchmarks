import random
import string
from collections import namedtuple
from copy import deepcopy

include_ranges = [
    (0x0021, 0x0021),
    (0x0023, 0x0026),
    (0x0028, 0x007E),
    (0x00A1, 0x00AC),
    (0x00AE, 0x00FF),
    (0x0100, 0x017F),
    (0x0180, 0x024F),
    (0x2C60, 0x2C7F),
    (0x16A0, 0x16F0),
    (0x0370, 0x0377),
    (0x037A, 0x037E),
    (0x0384, 0x038A),
    (0x038C, 0x038C),
]
unicode_alphabet = [
    chr(code_point) for current_range in include_ranges
    for code_point in range(current_range[0], current_range[1] + 1)
]
Book = namedtuple('Book', [
    'title', 'author', 'sales', 'is_published', 'languages', 'reviews', 'price'
])
Review = namedtuple('Review', ['author', 'comment'])


def get_json_data(n=1000, float_ndigits=8):
    languages = ('en', 'he', 'es', 'de')
    max_sales = int(2 ** 31 - 1)

    # for reproducibility
    random.seed(42)

    books = [
        {
            'title': randstring(),
            'author': randstring(),
            'sales': random.randint(0, max_sales),
            'is_published': random.choice((True, False)),
            'languages': random.sample(languages, k=random.randint(1, 4)),
            'reviews': [
                {
                    'author': randstring(),
                    'comment': randunicode(),
                } for _ in range(random.randint(0, 4))
            ],
            'price': round(random.random() * 100, float_ndigits),
        } for _ in range(n)
    ]
    return books


def get_tuples_data(data):
    data = deepcopy(data)
    for x in data:
        x['reviews'] = [list(Review(**r)) for r in x['reviews']]
    return [list(Book(**x)) for x in data]


def randstring():
    slen = random.randint(0, 24)
    return ''.join(random.choices(string.printable, k=slen))


def randunicode():
    slen = random.randint(0, 100)
    return ''.join(random.choices(unicode_alphabet, k=slen))
