# coding: utf-8
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import freeze_support


def counter(path):
    d = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line and "\u4e00" <= line[0] <= "\u9fff":
                word, freq = line.strip().split()
                word = word.strip()
                freq = int(freq.strip())
                if word not in d:
                    d[word] = 0
                d[word] += freq
    return d


def main():
    all_d = {}
    files = [x for x in Path("./out").iterdir() if x.is_file()]
    with ProcessPoolExecutor() as pool:
        for d in pool.map(counter, files):
            for word, freq in d.items():
                if word not in all_d:
                    all_d[word] = 0
                all_d[word] += freq
    sorted_word = dict(sorted(all_d.items(), key=lambda item: item[1], reverse=True))
    with open(
        "./chinese_word_frequency.txt", "w", encoding="utf-8", newline="\n"
    ) as fw:
        for word, freq in sorted_word.items():
            if len(word) < 2 or freq < 1000:
                continue
            fw.write(f"{word}\t{freq}\n")


if __name__ == "__main__":
    freeze_support()
    main()
