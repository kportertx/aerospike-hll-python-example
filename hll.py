#!/usr/bin/python

import argparse
import random
from itertools import combinations

import aerospike
from aerospike_helpers.operations import hll_operations
import generator


parser = argparse.ArgumentParser(description="HLL example")
parser.add_argument("--host", dest="host", default="127.0.0.1")
args = parser.parse_args()

HLL_BIN = "hll_bin"
HLL_INDEX_BITS = 12
HLL_MH_BITS = 0
MONTHS = 1


def init(host: str, port: int) -> aerospike.client:
    config = {"hosts": [(host, port)], "policies": {"timeout": 1000}}  # milliseconds

    client = aerospike.client(config).connect()
    return client


def getkey(name: str, time: int) -> tuple:
    return ("test", "hll", f"{name}:{time}")


# Ingest records with id in the range [start, end).
def ingest_month(client: aerospike.client, start: int, end: int, month: int) -> None:
    # We don't need to initialise HLL bins; performing a HLL add to an empty bin
    # will initialise it.
    print(f"Ingest ids {start}-{end} month {month}")

    for i in range(start, end):
        profile = generator.get_profile(i, month)
        # print("Profile {i}: {profile}")
        for tag in profile:
            ops = [hll_operations.hll_add(HLL_BIN, [str(i)], HLL_INDEX_BITS)]

            _, _, result = client.operate(getkey(tag, month), ops)


def ingest(client: aerospike.client) -> None:
    for i in range(MONTHS):
        ingest_month(client, 1, 20_000, i)


def count(client: aerospike.client, tag: str, month: int) -> None:
    ops = [hll_operations.hll_get_count(HLL_BIN)]
    _, _, result = client.operate(getkey(tag, month), ops)
    print(f"tag:{tag} month:{month} count:{result[HLL_BIN]}")


def get_union_count(client: aerospike.client, tag: str, t0: int, months: int) -> None:
    if months < 2:
        return

    times = range(t0 + 1, months)
    hlls = [
        record[2][HLL_BIN]
        for record in client.get_many([getkey(tag, time) for time in times])
    ]

    ops = [hll_operations.hll_get_union_count(HLL_BIN, hlls)]

    _, _, result = client.operate(getkey(tag, t0), ops)
    print(f"tag:{tag} months:{t0}-{t0 + months - 1} count:{result[HLL_BIN]}")


def get_intersect_count(client: aerospike.client, tags: list, month: int) -> None:
    hlls = [
        record[2][HLL_BIN]
        for record in client.get_many([getkey(tag, month) for tag in tags[1:]])
    ]

    ops = [hll_operations.hll_get_intersect_count(HLL_BIN, hlls)]

    _, _, result = client.operate(getkey(tags[0], month), ops)
    print(f"tags:{tags} month:{month} count:{result[HLL_BIN]}")


def get_intersect_count0(client: aerospike.client, tags: list, month: int) -> None:
    assert len(tags) == 3

    hlls = [
        record[2][HLL_BIN]
        for record in client.get_many([getkey(tag, month) for tag in tags])
    ]

    counts = [
        client.operate(
            getkey(tag, month),
            [hll_operations.hll_get_count(HLL_BIN)]
        )[2][HLL_BIN]
        for tag in tags
    ]

    ucount = client.operate(
        getkey(tags[0], month),
        [hll_operations.hll_get_union_count(HLL_BIN, hlls)]
    )[2][HLL_BIN]

    indicies = range(len(tags))
    combos = sorted([list(set(indicies) - set(combo)) + [combo]
              for combo in combinations(indicies, 2)])
    data = list(((key, [hlls[a], hlls[b]]) for key, (a, b) in combos))
    i2counts = []

    for _, (a, b) in combos:
        ops0 = [hll_operations.hll_get_intersect_count(HLL_BIN, [hlls[b]])]
        _, _, result0 = client.operate(getkey(tags[a], month), ops0)
        ops1 = [hll_operations.hll_get_intersect_count(HLL_BIN, [hlls[a]])]
        _, _, result1 = client.operate(getkey(tags[b], month), ops1)
        i2counts += [
            (
                (f"{a}&{b}", result0[HLL_BIN]),
                (f"{b}&{a}", result1[HLL_BIN])
            )
        ]

        assert result0[HLL_BIN] == result1[HLL_BIN]

        del (ops0, ops1, result0, result1)

    i2repeat2counts = []

    for i in indicies:
        ops = [hll_operations.hll_get_intersect_count(HLL_BIN, [hlls[i]])]
        _, _, result = client.operate(getkey(tags[i], month), ops)
        i2repeat2counts += [
            (
                (f"{i}&{i}", result[HLL_BIN])
            )
        ]

        del (ops, result)


    i3counts = []

    for key, thlls in data:
        ops = [hll_operations.hll_get_intersect_count(HLL_BIN, thlls)]

        _, _, result = client.operate(getkey(tags[key], month), ops)
        i3counts += [(f"{key}&?&?", result[HLL_BIN])]
        print(f"key:{key} tags:{tags} month:{month} i3count:{result[HLL_BIN]}")

    i3repeat2counts = []

    for _, pair in combos:
        a, b = pair
        ops0 = [hll_operations.hll_get_intersect_count(HLL_BIN, [hlls[a], hlls[b]])]
        _, _, result0 = client.operate(getkey(tags[pair[0]], month), ops0)
        ops1 = [hll_operations.hll_get_intersect_count(HLL_BIN, [hlls[a], hlls[b]])]
        _, _, result1 = client.operate(getkey(tags[pair[1]], month), ops1)
        i3repeat2counts += [
            (
                (f"{a}&{a}&{b}", result0[HLL_BIN]),
                (f"{b}&{a}&{b}", result1[HLL_BIN])
            )
        ]

        del (ops0, ops1, result0, result1)


    i3repeat3counts = []

    for i in indicies:
        ops = [hll_operations.hll_get_intersect_count(HLL_BIN, [hlls[i], hlls[i]])]
        _, _, result = client.operate(getkey(tags[i], month), ops)
        i3repeat3counts += [
            (
                (f"{i}&{i}&{i}", result[HLL_BIN])
            )
        ]

        del (ops, result)


    print(f"ucount:{ucount}")
    print(f"counts:{counts}")
    print(f"i2counts:{i2counts}")
    print(f"i2repeat2counts:{i2repeat2counts}")
    print(f"i3counts:{i3counts}")
    print(f"i3repeat2counts:{i3repeat2counts}")
    print(f"i3repeat3counts:{i3repeat3counts}")


def main() -> None:
    print(f"SEED: {generator.SEED}")
    client = init(args.host, 3000)
    ingest(client)

    # Get counts
    for i in range(MONTHS):
        count(client, "aerospike", i)

    # Get union
    get_union_count(client, "aerospike", 0, MONTHS)

    # Get intersection
    count(client, "vancouver", 0)
    count(client, "canada", 0)
    count(client, "washington", 0)
    get_intersect_count(client, ["vancouver", "washington"], 0)
    get_intersect_count0(client, ["canada", "vancouver", "washington"], 0)


if __name__ == "__main__":
    main()
