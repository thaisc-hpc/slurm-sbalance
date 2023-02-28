#!/usr/bin/env python
# Copyright (c) 2019-2023 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
# sbalance: query remaining slurm billing balance
from __future__ import print_function

import getpass
import subprocess
import argparse
import math
import csv
import json
import sys

from .config import __version__, __author__, __license__, SACCT_BEGIN_DATE
from .utils import VerboseLog, Verbosity
from .slurm import Slurm

class FieldConfig:
    def __init__(self, field:str, header:str, size:int, str_disp:str, field_disp:str):
        self.field = field
        self.header = header
        self.size = size
        self._str_disp = str_disp
        self._field_disp = field_disp

    @property
    def str_disp(self) -> str:
        return self._str_disp % self.size
    
    @property
    def field_disp(self) -> str:
        return self._field_disp % self.size

FIELD_CONFIGS={
    "account": FieldConfig(field="account", header="Account", size=10, str_disp="{:<%d}", field_disp="{:<%d}"),
    "description": FieldConfig(field="description", header="Description", size=12, str_disp= "{:<%d}", field_disp="{:<%d}"),
    "allocation_raw": FieldConfig(field="su_alloc_raw", header="Allocation(SU)", size=14, str_disp= "{:>%d}", field_disp="{:>%d.2f}"),
    "remaining_raw": FieldConfig(field="su_remaining_raw", header="Remaining(SU)", size=12, str_disp= "{:>%d}", field_disp="{:>%d.2f}"),
    "used_raw": FieldConfig(field="su_used_raw", header="Used(SU)", size=12, str_disp= "{:>%d}", field_disp="{:>%d.2f}"),
    "remaining_percent": FieldConfig(field="percent_remaining", header="Remaining(%)", size=12, str_disp="{:>%d}", field_disp="{:>%d.2%%}")
}

DISPLAY_FIELDS=[
    "account",
    "description",
    "allocation_raw",
    "remaining_raw",
    "remaining_percent",
    "used_raw"
]

def parse_args():
    slurm_version = str(subprocess.check_output(['sinfo', '--version']).decode())

    parser = argparse.ArgumentParser(prog='sbalance', description='Query slurm account balance.')
    version = "sbalance " + __version__ + " with " + slurm_version

    parser.add_argument(
        '-d','--detail', action='store_true', help="display SU usage per users")
    parser.add_argument(
        '-S', '--start', action='store', default=SACCT_BEGIN_DATE, help="starting date")
    parser.add_argument(
        '-o', '--output', action='store', help="output file")
    
    format_parser = parser.add_argument_group('format', 'output format')
    format_parser.add_argument(
        '--format', action='store', dest='format', help="output format. Valid options: table, csv, json. Default: table", default='table')
    format_parser.add_argument(
        '-c', '--csv', action='store_const', dest='format', const='csv', help="print output as csv")
    format_parser.add_argument(
        '-t', '--table', action='store_const', dest='format', const='table', help="print output as table")
    format_parser.add_argument(
        '-j', '--json', action='store_const', dest='format', const='json', help="print output as json")
    format_parser.add_argument(
        '-k', action='store_const', dest='unit', default='', const='k', help="show output in kSU (1,000 SU)")
    format_parser.add_argument(
        '-m', action='store_const', dest='unit', const='M', help="show output in MSU (1,000,000 SU)")
    
    parser.add_argument(
        '-v', '--verbose', action='count', help="verbose mode (multiple -v's increase verbosity)")
    parser.add_argument(
        '-V', '--version', action='version', version=version)

    return parser.parse_args()

def main():
    args = parse_args()  

    VerboseLog.set_verbose(args.verbose)

    if args.unit == 'k':
        su_units = 'kSU'
        su_factor = 1.0e-3
    elif args.unit == 'M':
        su_units = 'MSU'
        su_factor = 1.0e-6
    else:
        su_units = 'SU'
        su_factor = 1

    user = getpass.getuser()
    VerboseLog.print("User:     " + user, level=Verbosity.INFO)

    # Get billings usage from scontrol command
    usage = Slurm.get_usage()

    if args.format == 'table':
        # Build header 
        header_fields = []
        header_format = []
        row_format = []
        for f in DISPLAY_FIELDS:
            header_format.append(FIELD_CONFIGS[f].str_disp)
            header_fields.append(FIELD_CONFIGS[f].header)
            row_format.append(FIELD_CONFIGS[f].field_disp)

        header = " ".join(header_format).format(*header_fields)
        row_str = " ".join(header_format)
        row_field = " ".join(row_format)
        print()
        print(header)
        print('-' * len(header))
        for u in usage:
            row_fields = []
            for f in DISPLAY_FIELDS:
                row_fields.append(u[FIELD_CONFIGS[f].field])
            
            if u['su_alloc_raw'] == 'unlimited':
                print(row_str.format(*row_fields))
            else:
                print(row_field.format(*row_fields))
        print()
    elif args.format == 'json':
        j = json.dumps(usage)
        print(j)
