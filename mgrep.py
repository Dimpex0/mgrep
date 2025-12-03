import argparse
import re
import os
import shutil
import sys
import time
from argparse import Namespace
from multiprocessing import Pool
from pathlib import Path

import requests

TEMP_DIR = Path("temp")

class InvalidFileError(RuntimeError):
    pass

class InvalidAmountOfWorkers(RuntimeError):
    pass


def setup_cli() -> Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "pattern",
        help="Шаблон, по който да се търси"
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="Файлове, в които да се търси"
    )
    parser.add_argument(
        "-n", "--line-number",
        action="store_true",
        help="Показване на номерата на редовете със съвпадения"
    )
    parser.add_argument(
        "-m", "--in-memory",
        action="store_true",
        help="Зареждане на целия файл в паметта преди търсене"
    )
    parser.add_argument(
        "-p", "--parallel",
        type=int,
        default=0,
        help="Брой паралелни търсения"
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-r", "--recursive",
        action="store_true",
        help="Рекурсивно търсене във всички файлове в директорията"
    )
    group.add_argument(
        "-u", "--from-url",
        action="store_true",
        help="Търсене във файлове от URL адреси"
    )

    return parser.parse_args()


def search_in_file(pattern: re.Pattern, file_path: str, is_in_memory: bool = False) -> list[tuple]:
    if not os.path.exists(file_path):
        raise InvalidFileError

    try:
        (_, filename) = os.path.split(file_path)
        result = []
        if is_in_memory:
            file = open(os.path.join(file_path))
            data = file.readlines()
            file.close()
            for index, line in enumerate(data):
                if re.search(pattern, line) is not None:
                    result.append((line.rstrip('\n'), file_path, index + 1))
        else:
            file = open(os.path.join(file_path))
            line = file.readline()
            line_number = 1
            while line != '':
                if re.search(pattern, line) is not None:
                    result.append((line.rstrip('\n'), file_path, line_number))

                line = file.readline()
                line_number += 1

        return result
    except UnicodeDecodeError:
        print(f"Couldn't open file: {file_path} Unsupported encoding")


def run_multi_threaded(pattern: re.Pattern, file_paths: list[str], is_in_memory: bool = False, is_line_numbers: bool = False, amount_of_workers: int = 0):
    if amount_of_workers < 0 or (amount_of_workers > len(file_paths) and amount_of_workers != 0):
        raise InvalidAmountOfWorkers

    if amount_of_workers == 0:
        all_results = []
        for file in files:
            res = search_in_file(pattern, file, is_in_memory)
            all_results.append(res)

        return all_results

    with Pool(amount_of_workers) as pool:
        args = [(pattern, file_path, is_in_memory) for file_path in files]
        results = pool.starmap(search_in_file, args)

    return []

def save_url_to_temp(url):

    filename = TEMP_DIR / os.path.basename(url)
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()

        with open(filename, "wb") as f:
            f.write(r.content)

        return str(filename)
    except Exception as e:
        print(f"Error when reading from {url}")

def fetch_files_from_url(urls: list[str]):
    TEMP_DIR.mkdir(exist_ok=True)

    with Pool() as pool:
        local_files = pool.map(save_url_to_temp, urls)

    return [f for f in local_files if f is not None]

if __name__ == "__main__":
    args: Namespace = setup_cli()
    pattern: re.Pattern = args.pattern
    files: list[str]    = args.files
    line_number: bool   = args.line_number
    in_memory: bool     = args.in_memory
    parallel: int       = args.parallel
    recursive: bool     = args.recursive
    from_url: bool      = args.from_url

    # file = open(os.path.join("D:\\", "Projects", "text.txt"))
    # data = file.read()
    # file.close()
    #
    # for i in range(0, 100):
    #     with open(os.path.join("D:\\", "Projects", f"text-{i}.txt"), "w") as f:
    #         f.write(data)


    start = time.time()
    if not files:
        sys.exit("No files provided")

    if from_url and not recursive:
        files = fetch_files_from_url(files)

    if not files:
        sys.exit("Couldn't get any of the given urls")

    files = [file for file in os.listdir(os.curdir) if os.path.isfile(file)]
    # print(files)
    if not files:
        sys.exit("No files in the current directory")

    try:
        results = run_multi_threaded(pattern, files, in_memory, line_number, parallel)
        end: float = time.time()
        print(f"It took {(end - start):.2f} seconds")

        # print(results)
        # for line, filename, number in results:
        #     if line_number:
        #         print(f"{filename}:{number} - {line}")
        #     else:
        #         print(f"{line}")
    finally:
         if TEMP_DIR.exists():
             shutil.rmtree(TEMP_DIR)