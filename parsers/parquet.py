import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path
from collections import defaultdict
import re


def create_parquet():
  all_data = defaultdict(list)

  LICENSE = """ENCYCLOPEDIA BRITANNICA, SEVENTH EDITION: A MACHINE-READABLE TEXT
  TRANSCRIPTION, v1.0, The Nineteenth-Century Knowledge Project, 2022.
  nckp@temple.edu, https://tu-plogan.github.io/

  Source: Encyclopaedia Britannica: A Dictionary of Arts, Sciences,
  and General Literature. Edited by T. S. Baynes. 9th ed. 25 vols.
  NY: Charles Scribner's Sons, 1878-1889.

  License: This work is licensed under the Creative Commons Attribution
  4.0 International License. To view a copy of this license, visit
  http://creativecommons.org/licenses/by/4.0/ or send a letter to
  Creative Commons, PO Box 1866, Mountain View, CA 94042, USA."""

  for file in Path("eb07/TXT").glob("*/*.txt"):
    text = file.open().read()
    try:
      empty, header, body = text.split("+=====================================================================+")
    except ValueError:
      print(file)
      raise
    last_line = header.split("\n")[-2]
    location = last_line.split("[")[-1]
    # Drop trailing characters
    location = location.rstrip().rstrip("]")
    edition, volume, page = map(int, location.split(":"))
    body = body.strip()
    all_data["text"].append(body)
    all_data["edition"].append(edition)
    all_data["volume"].append(volume)
    all_data["page"].append(page)
    all_data["file"].append(str(file))
  frame = pa.table(all_data)
  frame.replace_schema_metadata({"license": LICENSE})
  pq.write_table(frame, "eb07/eb07.parquet", compression="zstd")

if __name__ == "__main__":
  create_parquet()
  tb = pq.read_table("eb07/eb07.parquet")
  title = pc.extract_regex(tb['text'], "(?P<title>^(?: ?\p{Lu}+)+)").flatten()[0]
  tb = tb.append_column("title", title)
  tb = tb.sort_by([('edition', "ascending"), ('volume', "ascending"), ('page', "ascending"), ('title', "ascending")])
  pq.write_table(tb, "eb07/eb07.parquet", compression="zstd")

