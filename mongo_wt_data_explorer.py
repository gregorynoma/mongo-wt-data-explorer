import subprocess
import binascii
import bson
import sys
import pprint

wt_path = ""
ksdecode_path = ""
data_path = ""

if not wt_path:
    wt_path = input("Path to wt: ")
if not ksdecode_path:
    ksdecode_path = input("Path to ksdecode (optional): ")
if not data_path:
    data_path = input("Path to data files: ")


def process_dump(
    proc,
    handle_key=lambda key: None,
    handle_value=lambda value: None,
    handle_key_value=lambda key, value: None,
):
    while True:
        line = proc.stdout.readline().decode("utf-8").strip()

        if not line:
            print("No data section")
            sys.exit(1)

        if line == "Data":
            break

    while True:
        key = proc.stdout.readline().strip()

        if not key:
            break

        value = proc.stdout.readline().strip()

        handle_key(key)
        handle_value(value)
        handle_key_value(key, value)


def dump(ident):
    return subprocess.Popen(
        [
            wt_path,
            "-r",
            "-C",
            "log=(compressor=snappy,path=journal)",
            "-h",
            data_path,
            "dump",
            "-x",
            "table:" + ident,
        ],
        stdout=subprocess.PIPE,
    )


def dump_write(
    ident,
    decode_key=lambda key: key,
    decode_value=lambda value: value,
    extra=lambda write, key, value: None,
):
    dump_ident = dump(ident)

    def write_key(write, key):
        write("Key:\t%s\n" % (decode_key(key),))

    def write_value(write, value):
        write("Value:\t%s\n" % (decode_value(value),))

    def run_extra(write, key, value):
        extra(write, key, value)

    file = input("File to write (leave empty to print): ")
    if file:
        with open(file, "w") as f:
            process_dump(
                dump_ident,
                lambda key: write_key(f.write, key),
                lambda value: write_value(f.write, value),
                lambda key, value: run_extra(f.write, key, value),
            )
    else:

        def print_without_newline(value):
            print(value, end="")

        process_dump(
            dump_ident,
            lambda key: write_key(print_without_newline, key),
            lambda value: write_value(print_without_newline, value),
            lambda key, value: run_extra(print_without_newline, key, value),
        )

    dump_ident.wait()


def decode_to_bson(data):
    return bson.decode(binascii.a2b_hex(data))


def format_to_bson(data):
    return "\n\t" + pprint.pformat(decode_to_bson(data)).replace("\n", "\n\t")


def get_string_width(text):
    return max(map(lambda line: len(line), text.splitlines()))


def explore_index(entry, index, position):
    collection_msg = "Collection " + entry["ns"]
    index_msg = "Index " + index
    header_width = max(len(collection_msg), len(index_msg))

    while True:
        print("*" * header_width)
        print(collection_msg)
        print(index_msg)
        print("*" * header_width)
        print("(b) back")
        print("(c) catalog entry")
        print("(d) dump index")
        print("(i) ident")
        print("(q) quit")

        def get_catalog_entry():
            return entry["md"]["indexes"][position]

        cmd = input("Choose something to do: ")

        if cmd == "b":
            return

        elif cmd == "c":
            print(pprint.pformat(get_catalog_entry()))

        elif cmd == "d":

            def write_decoded_key(write, key, value):
                if not ksdecode_path:
                    return

                def get_rid_type():
                    if index == "_id_":
                        return "none"
                    elif "clusteredIndex" in entry["md"]["options"]:
                        return "string"
                    else:
                        return "long"

                rid_type = get_rid_type()
                ksdecode = subprocess.run(
                    [
                        ksdecode_path,
                        "-o",
                        "bson",
                        "-p",
                        pprint.pformat(get_catalog_entry()["spec"]["key"]),
                        "-t",
                        value,
                        "-r",
                        rid_type,
                        key,
                    ],
                    capture_output=True,
                )
                write("Decoded:\n\t" + ksdecode.stdout.decode("utf-8").strip() + "\n")

            dump_write(entry["idxIdent"][index], extra=write_decoded_key)

        elif cmd == "i":
            print(entry["idxIdent"][index])

        elif cmd == "q":
            sys.exit(0)

        else:
            print("Unrecognized command " + cmd)


def explore_collection(entry):
    collection_msg = "Collection " + entry["ns"]
    header_width = len(collection_msg)

    indexes = []
    for index in entry["idxIdent"]:
        indexes.append(index)

    while True:
        print("*" * header_width)
        print(collection_msg)
        print("*" * header_width)
        print("(b) back")
        print("(c) catalog entry")
        print("(d) dump collection")
        print("(i) ident")
        print("(q) quit")

        for i, index in enumerate(indexes):
            print("(" + str(i) + ") " + index)

        cmd = input("Choose something to do: ")

        if cmd == "b":
            return

        elif cmd == "c":
            print(pprint.pformat(entry))

        elif cmd == "d":
            dump_write(entry["ident"], decode_value=format_to_bson)

        elif cmd == "i":
            print(entry["ident"])

        elif cmd == "q":
            sys.exit(0)

        elif cmd.isnumeric() and int(cmd) < len(entries):
            explore_index(entry, indexes[int(cmd)], int(cmd))

        else:
            print("Unrecognized command " + cmd)


dump_catalog = dump("_mdb_catalog")
entries = []
process_dump(
    dump_catalog, handle_value=lambda entry: entries.append(decode_to_bson(entry))
)
dump_catalog.wait()

catalog_msg = "Catalog"
header_width = len(catalog_msg)

while True:
    print("*" * header_width)
    print(catalog_msg)
    print("*" * header_width)
    print("(d) dump catalog")
    print("(q) quit")

    for i, entry in enumerate(entries):
        print("(" + str(i) + ") " + entry["ns"])

    cmd = input("Choose something to do: ")

    if cmd == "d":
        dump_write("_mdb_catalog", decode_value=format_to_bson)

    elif cmd == "q":
        sys.exit(0)

    elif cmd.isnumeric() and int(cmd) < len(entries):
        explore_collection(entries[int(cmd)])

    else:
        print("Unrecognized command " + cmd)
