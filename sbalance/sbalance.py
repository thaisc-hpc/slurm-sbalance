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
    def __init__(self, field:str, header:str, topic:str=None, size:int=10, str_disp:str="{:<%d}", field_disp:str="{:<%d}"):
        self.field =field
        self.header =header
        self.topic =topic
        self.size =size
        self._str_disp =str_disp
        self._field_disp =field_disp
        self._map =map

    @property
    def str_disp(self) -> str:
        return self._str_disp % self.size
    
    @property
    def field_disp(self) -> str:
        return self._field_disp % self.size

FIELD_CONFIGS={
    "account": FieldConfig(field="account", header="Account", size=10, str_disp="{:<%d}", field_disp="{:<%d}"),
    "description": FieldConfig(field="description", header="Descr", size=7, str_disp="{:<%d}", field_disp="{:<%d}"),

    # Service Hour
    "allocation": FieldConfig(field="sh_alloc", header="Allocation", size=10, topic="Service Hour", str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "remaining": FieldConfig(field="sh_remaining", header="Remaining", size=10, topic="Service Hour", str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "used": FieldConfig(field="sh_used", header="Used", size=10, topic="Service Hour", str_disp="{:>%d}", field_disp="{:>%d.2f}"),

    # System SU
    "allocation_system": FieldConfig(field="su_alloc", header="Allocation(SU)", size=14, str_disp="{:>%d}", field_disp="{:>%d.0f}"),
    "remaining_system": FieldConfig(field="su_remaining", header="Remaining(SU)", size=12, str_disp="{:>%d}", field_disp="{:>%d.0f}"),
    "used_system": FieldConfig(field="su_used", header="Used(SU)", size=12, str_disp="{:>%d}", field_disp="{:>%d.0f}"),

    "remaining_percent": FieldConfig(field="percent_remaining", header="Remaining(%)", size=12, str_disp="{:>%d}", field_disp="{:>%d.2%%}"),
    
    "allocation_compute": FieldConfig(field="su_alloc_compute",  header="Alloc(Node-Hr)", topic="Compute (Estimated)", size=14, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "remaining_compute": FieldConfig(field="su_remaining_compute", header="Remain(Node-Hr)", topic="Compute (Estimated)", size=14, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "used_compute": FieldConfig(field="su_used_compute", header="Used(Node-Hr)", topic="Compute (Estimated)", size=14, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    
    "allocation_gpu": FieldConfig(field="su_alloc_gpu", header="Alloc(Node-Hr)", topic="GPU (Estimated)", size=14, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "remaining_gpu": FieldConfig(field="su_remaining_gpu", header="Remain(Node-Hr)", topic="GPU (Estimated)", size=14, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "used_gpu": FieldConfig(field="su_used_gpu", header="Used(Node-Hr)", topic="GPU (Estimated)", size=14, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    
    "allocation_memory": FieldConfig(field="su_alloc_memory", header="Allocation(Hr)", topic="Memory (Estimated)", size=14, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "remaining_memory": FieldConfig(field="su_remaining_memory", header="Remaining(Hr)", topic="Memory (Estimated)", size=14, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "used_memory": FieldConfig(field="su_used_memory", header="Used(Hr)", size=14, topic="Memory (Estimated)", str_disp="{:>%d}", field_disp="{:>%d.2f}"),
}

DISPLAY_FIELDS=[
    "account",
    "description",
    "remaining_percent",
    "allocation",
    "remaining",
    "used",

    # "allocation_system",
    # "remaining_system",
    # "used_system",

    # "allocation_compute",
    # "remaining_compute",
    #"used_compute",

    # "allocation_gpu",
    # "remaining_gpu",
    #"used_gpu",

    # "allocation_memory",
    # "remaining_memory",
    # "used_memory"
]

def parse_args():
    slurm_version =str(subprocess.check_output(['sinfo', '--version']).decode())

    parser =argparse.ArgumentParser(prog='sbalance', description='Query slurm account balance.')
    version ="sbalance " + __version__ + " with " + slurm_version

    parser.add_argument(
        '-d','--detail', action='store_true', help="display SU usage per users")
    parser.add_argument(
        '-S', '--start', action='store', default=SACCT_BEGIN_DATE, help="starting date")
    parser.add_argument(
        '-o', '--output', action='store', help="output file")
    
    format_parser =parser.add_argument_group('format', 'output format')
    format_parser.add_argument(
        '--format', action='store', dest='format', help="output format. Valid options: table, csv, json. Default: table", default='table')
    format_parser.add_argument(
        '-c', '--csv', action='store_const', dest='format', const='csv', help="print output as csv")
    format_parser.add_argument(
        '-t', '--table', action='store_const', dest='format', const='table', help="print output as table")
    format_parser.add_argument(
        '-j', '--json', action='store_const', dest='format', const='json', help="print output as json")
    # format_parser.add_argument(
    #     '-k', action='store_const', dest='unit', default='', const='k', help="show output in kSU (1,000 SU)")
    # format_parser.add_argument(
    #     '-m', action='store_const', dest='unit', const='M', help="show output in MSU (1,000,000 SU)")
    
    parser.add_argument(
        '-v', '--verbose', action='count', help="verbose mode (multiple -v's increase verbosity)")
    parser.add_argument(
        '-V', '--version', action='version', version=version)

    return parser.parse_args()

def main():
    args =parse_args()  

    VerboseLog.set_verbose(args.verbose)

    # if args.unit =='k':
    #     su_units ='kSU'
    #     su_factor =1.0e-3
    # elif args.unit =='M':
    #     su_units ='MSU'
    #     su_factor =1.0e-6
    # else:
    #     su_units ='SU'
    #     su_factor =1

    user = getpass.getuser()
    VerboseLog.print("User:     " + user, level=Verbosity.INFO)

    # Get billings usage from scontrol command
    usage =Slurm.get_usage()

    if args.format =='table':
        # Build header 
        header_fields = []
        header_format = []
        row_format = []

        # Formatting table headers
        cur_topic = ""
        last_topic = ""
        last_idx = 0
        topic_format = []
        topic_fields = []
        for f in DISPLAY_FIELDS:
            if FIELD_CONFIGS[f].topic !=None:
                if cur_topic !=FIELD_CONFIGS[f].topic:
                    # Enter new topic
                    header ="|".join(header_format).format(*header_fields)
                    if len(header) > 0:
                        topic_format.append("{:^%d}" % (len(header) - last_idx))
                        topic_fields.append(cur_topic)
                    last_idx = len(header) + 1 # Add 1 for space between header
                    last_topic = cur_topic
                    cur_topic = FIELD_CONFIGS[f].topic
            else:
                if cur_topic !="":
                    # Enter new topic
                    header =" ".join(header_format).format(*header_fields)
                    if len(header) > 0:
                        topic_format.append("{:^%d}" % (len(header) - last_idx))
                        topic_fields.append(cur_topic)
                    last_idx = len(header) + 1 # Add 1 for space between header
                    last_topic = cur_topic
                    cur_topic = ""
                    
            header_format.append(FIELD_CONFIGS[f].str_disp)
            header_fields.append(FIELD_CONFIGS[f].header)
            row_format.append(FIELD_CONFIGS[f].field_disp)

        if cur_topic != last_topic:
            header =" ".join(header_format).format(*header_fields)
            topic_format.append("{:^%d}" % (len(header) - last_idx))
            topic_fields.append(cur_topic)

        header = " ".join(header_format).format(*header_fields)
        row_str = " ".join(header_format)
        row_field = " ".join(row_format)
        print()
        if len(topic_fields) > 1:
            print(" ".join(topic_format).format(*topic_fields))
        print(header)
        print('-' * len(header))
        for u in usage:
            row_fields = []
            for f in DISPLAY_FIELDS:
                row_fields.append(u[FIELD_CONFIGS[f].field])
            
            if u['su_alloc'] == 'unlimited':
                print(row_str.format(*row_fields))
            else:
                print(row_field.format(*row_fields))
        print('-' * len(header))
        print()
    elif args.format == 'json':
        j = json.dumps(usage)
        print(j)
