# -*- coding: utf-8 -*-
import functools
import json
from pathlib import Path
from multiprocessing import freeze_support
from concurrent.futures import ProcessPoolExecutor

import jieba


def load_baike(path):
    path = Path(path).expanduser().absolute()
    counter = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip().replace('\n', '')
            data = json.loads(line.strip())
            if data["title"]:
                for word in jieba.cut(data["title"]):
                    if word not in counter:
                        counter[word] = 0
                    counter[word] += 1
            if data["desc"]:
                for word in jieba.cut(data["desc"]):
                    if word not in counter:
                        counter[word] = 0
                    counter[word] += 1
            if data["answer"]:
                for word in jieba.cut(data["answer"]):
                    if word not in counter:
                        counter[word] = 0
                    counter[word] += 1
    with open(f"./out/{path.stem}.txt", "w", encoding="utf-8") as f:
        for word, freq in counter.items():
            f.write(f"{word}\t{freq}\n")


def load_news(path):
    path = Path(path).expanduser().absolute()
    counter = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip().replace('\n', '')
            data = json.loads(line.strip())
            if data["keywords"]:
                for word in jieba.cut(data["keywords"]):
                    if word not in counter:
                        counter[word] = 0
                    counter[word] += 1
            if data["title"]:
                for word in jieba.cut(data["title"]):
                    if word not in counter:
                        counter[word] = 0
                    counter[word] += 1
            if data["content"]:
                for word in jieba.cut(data["content"]):
                    if word not in counter:
                        counter[word] = 0
                    counter[word] += 1
    with open(f"./out/{path.stem}.txt", "w", encoding="utf-8") as f:
        for word, freq in counter.items():
            f.write(f"{word}\t{freq}\n")


def load_webtext(path):
    path = Path(path).expanduser().absolute()
    counter = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip().replace('\n', '')
            data = json.loads(line.strip())
            if data["title"]:
                for word in jieba.cut(data["title"]):
                    if word not in counter:
                        counter[word] = 0
                    counter[word] += 1
            if data["desc"]:
                for word in jieba.cut(data["desc"]):
                    if word not in counter:
                        counter[word] = 0
                    counter[word] += 1
            if data["content"]:
                for word in jieba.cut(data["content"]):
                    if word not in counter:
                        counter[word] = 0
                    counter[word] += 1
    with open(f"./out/{path.stem}.txt", "w", encoding="utf-8") as f:
        for word, freq in counter.items():
            f.write(f"{word}\t{freq}\n")


def load_wiki(path):
    path = Path(path).expanduser().absolute()
    counter = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip().replace('\n', '')
            data = json.loads(line.strip())
            if data["text"]:
                for word in jieba.cut(data["text"]):
                    if word not in counter:
                        counter[word] = 0
                    counter[word] += 1
    with open(f"./out/{path.parent.stem}_{path.stem}.txt", "w", encoding="utf-8") as f:
        for word, freq in counter.items():
            f.write(f"{word}\t{freq}\n")


def main():
    with ProcessPoolExecutor() as pool:
        futures = []
        for fp in Path("./data/baike").iterdir():
            if fp.is_file():
                futures.append(pool.submit(load_baike, str(fp)))
        for fp in Path("./data/news").iterdir():
            if fp.is_file():
                futures.append(pool.submit(load_news, str(fp)))
        for fp in Path("./data/webtext").iterdir():
            if fp.is_file():
                futures.append(pool.submit(load_webtext, str(fp)))
        for fp in Path("./data/wiki_zh").glob("**/*"):
            if fp.is_file():
                futures.append(pool.submit(load_wiki, str(fp)))
    for task in futures:
        counter = task.result()


if __name__ == "__main__":
    freeze_support()
    jieba.initialize()
    main()
