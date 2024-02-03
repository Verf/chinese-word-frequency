# coding: utf-8
import sys
from pathlib import Path


def main(argv):
    infile = Path(argv[1]).expanduser().absolute()
    with open(infile, "r", encoding="utf-8") as f:
        count = 0
        file_count = 0
        fw = open(
            Path(argv[2], f"{infile.stem}_{file_count}{infile.suffix}"),
            "w",
            encoding="utf-8",
            newline="\n",
        )
        for line in f:
            fw.write(line)
            count += 1
            if count >= 10000:
                count = 0
                file_count += 1
                fw.close()
                fw = open(
                    Path(argv[2], f"{infile.stem}_{file_count}{infile.suffix}"),
                    "w",
                    encoding="utf-8",
                    newline="\n",
                )
        fw.close()


if __name__ == "__main__":
    main(sys.argv)
