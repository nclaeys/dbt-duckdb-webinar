#!/usr/bin/env python3
import os

import duckdb
import sys


if __name__ == '__main__':
  print(duckdb.sql(f"""select * from '{sys.argv[1]}' limit 10"""))