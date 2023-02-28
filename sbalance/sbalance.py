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
from .slurm import *

def parse_args():
    slurm_version = str(subprocess.check_output([SINFO_CMD, '--version']).decode())

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

    # List accountable QoS    
    qos = get_slurm_qos()

    # List user accounts and associations
    def_qos = get_slurm_default_qos()

    # Get billings usage from scontrol command
    usage = get_slurm_usage(def_qos)

    if args.format == 'table':
        header = "{:<10} {:<12} {:>14} {:>12} {:>12} {:>12}".format("Account","Description", "Allocation(SU)","Remaining(SU)","Remaining(%)","Used(SU)")
        print()
        print(header)
        print('-' * len(header))
        for u in usage:
            if u['su_limit'] == 'unlimited':
                print("{:<10} {:<12} {:>14} {:>12} {:>12} {:>12}".format(
                    u['account'], 
                    qos[u['account']]['Descr'], 
                    u['su_limit'],
                    u['su_remaining'], 
                    u['percent_remaining'],
                    u['su_used'])
                )
            else:
                print("{:<10} {:<12} {:>14} {:>12} {:>12.2%} {:>12}".format(
                    u['account'],
                    qos[u['account']]['Descr'],
                    u['su_limit'],
                    u['su_remaining'],
                    u['percent_remaining'],
                    u['su_used'])
                )
        print()
    elif args.format == 'json':
        j = json.dumps(usage)
        print(j)
