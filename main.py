import subprocess
import csv
import time
from glob import glob
import tomllib
import sys
import argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import os


def generate_data_from_pcap(args):
    filename, category, timestamp, config = args
    inicio = time.time()

    categories = config["categories"].get(category)
    fields_table = {item["Key"]: item["Value"] for item in categories}
    if not fields_table:
        return f"[ERROR] Table not found for CAT {category}"

    field_keys = list(fields_table.keys())

    tshark_cmd = [
        config["tshark"]["path"],
        "-r", filename,
        *config["tshark"]["parameters"],
        "-Y", f"asterix.category=={category}",
    ]

    if timestamp:
        tshark_cmd += ["-e", "frame.time_epoch"]

    for v in fields_table.values():
        tshark_cmd += ["-e", v]

    try:
        process = subprocess.run(
            tshark_cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=True
        )
    except subprocess.CalledProcessError as e:
        return f"[ERROR] {Path(filename).name}: {e.stderr.strip()}"

    raw_data = process.stdout.strip()
    if not raw_data:
        return f"[WARN] {Path(filename).name}: empty output"

    outfile = Path(filename).stem + ".csv"

    buffer = []
    buffer_size = 10000

    with open(outfile, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)

        if timestamp:
            writer.writerow(["TIMESTAMP"] + field_keys)
        else:
            writer.writerow(field_keys)

        for line in raw_data.splitlines():
            buffer.append(line.split(";"))
            if len(buffer) >= buffer_size:
                writer.writerows(buffer)
                buffer.clear()

        if buffer:
            writer.writerows(buffer)

    return f"[OK] {Path(filename).name} → {outfile} ({time.time() - inicio:.2f}s)"


class PcapAsterix:
    def __init__(self):
        self.config = self.from_toml("config.toml")

        parser = argparse.ArgumentParser(description="ASTERIX PCAP → CSV (paralelo)")
        parser.add_argument("-f", "--file", help="Arquivo PCAP")
        parser.add_argument("-d", "--directory", help="Diretório PCAPs")
        parser.add_argument(
            "-c", "--category",
            choices=["21", "23", "34", "48", "62"],
            required=True
        )
        parser.add_argument("-ts", "--timestamp", action="store_true")
        parser.add_argument(
            "-j", "--jobs",
            type=int,
            default=os.cpu_count() // 2,
            help="Processos paralelos"
        )

        args = parser.parse_args()

        if not args.file and not args.directory:
            print("❌ Use -f ou -d")
            return

        if args.file:
            result = generate_data_from_pcap(
                (args.file, args.category, args.timestamp, self.config)
            )
            print(result)
            return

        files = glob(f"{args.directory}/*")
        if not files:
            print(f"⚠️ Nenhum PCAP em {args.directory}")
            return

        print(f"▶ Processando {len(files)} arquivos com {args.jobs} processos\n")

        jobs = [
            (f, args.category, args.timestamp, self.config)
            for f in files
        ]

        start = time.time()

        with ProcessPoolExecutor(max_workers=args.jobs) as executor:
            futures = [executor.submit(generate_data_from_pcap, j) for j in jobs]
            for future in as_completed(futures):
                print(future.result())

        print(f"\n✔ Finalizado em {time.time() - start:.2f}s")

    def from_toml(self, filepath):
        with open(filepath, "rb") as f:
            return tomllib.load(f)


if __name__ == "__main__":
    PcapAsterix()
