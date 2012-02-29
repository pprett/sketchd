import random

from itertools import cycle

from sketchd.algorithm import heavyhitters


alphabet = "abcdefghijklmnopqrstuvwxyz"
symbols = []
for i, c in enumerate(alphabet):
    symbols.extend([c] * (i+1)**2)


def test_spacesaving():
    ss = heavyhitters.SpaceSaving("abc")
    symbols = list(symbols)
    random.shuffle(symbols)

    c = len(symbols) * 4

    for s in cycle(symbols):
        ss.update(s)
        c -= 1
        if c == 0:
            break

    print(repr(ss.stream_summary))
