import msgpack
import os


def unpack_msgpack(path):
    with open(path, "rb") as f:
        data = f.read()
        files_dict = msgpack.unpackb(data, raw=False)
        for name, content in files_dict.items():
            fqn = os.path.join("/tmp/unpacked", name)
            with open(fqn, "wb") as fh:
                fh.write(content)
            print("unpacked " + fqn)


if __name__ == "__main__":
    unpack_msgpack("/tmp/packed/shard.0")
