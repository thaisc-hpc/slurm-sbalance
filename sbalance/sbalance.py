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

from .config import __version__, __author__, __license__, SACCT_BEGIN_DATE, DEFAULT_DISPLAY_FIELDS
from .utils import VerboseLog, Verbosity
from .slurm import Slurm
from .fields import FIELD_CONFIGS, FIELD_PER_USER_CONFIGS

def parse_args():
    slurm_version =str(subprocess.check_output(['sinfo', '--version']).decode())

    parser =argparse.ArgumentParser(prog='sbalance', description='Query slurm account balance.')
    version ="sbalance " + __version__ + " with " + slurm_version

    parser.add_argument(
        '-X','--exact', action='store_true', help="use sacct to gather jobs information. Could result in longer execution time.")
    parser.add_argument(
        '-d','--detail', action='store_true', dest="per_user", help="display usage per users. Override --output-format and imply use of -X option")
    parser.add_argument(
        '-S', '--start', action='store', default=SACCT_BEGIN_DATE, help="starting date")
    parser.add_argument(
        '--output', action='store', help="output file")
    parser.add_argument(
        '-o', '--format', help='comma separated list of fields.', type=lambda s: [field for field in s.split(',')])
    parser.add_argument(
        '--estimate', action='store', dest='estimate', choices=['compute', 'gpu', 'memory'], help="show estimated allocation and remaining usage for nodes in the specified partition")
    parser.add_argument(
        '-c', '--compute', help='show estimated allocation and remaining usage for compute nodes', action='store_const', dest="estimate", const="compute")
    parser.add_argument(
        '-g', '--gpu', help='show estimated allocation and remaining usage for GPU nodes', action='store_const', dest="estimate", const="gpu")
    parser.add_argument(
        '-m', '--memory', help='show estimated allocation and remaining usage for memory nodes', action='store_const', dest="estimate", const="memory")

    format_parser =parser.add_argument_group('format', 'output format')
    format_parser.add_argument(
        '--output-format', action='store', dest='output_format', choices=['table', 'csv', 'json'], help="output format. Default: table", default='table')
    format_parser.add_argument(
        '--csv', action='store_const', dest='output_format', const='csv', help="print output as csv")
    format_parser.add_argument(
        '--table', action='store_const', dest='output_format', const='table', help="print output as table")
    format_parser.add_argument(
        '--json', action='store_const', dest='output_format', const='json', help="print output as json")
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
    VerboseLog.print(args, level=Verbosity.DEBUG)

    # If detail is set to true, use sacct for gather account usage
    if args.per_user:
        args.exact = True

    if args.format != None:
        for field in args.format:
            if not field in FIELD_CONFIGS.keys():
                print('error: Invalid field: "{}"'.format(field))
                return
        display_fields = args.format
    else:
        display_fields = DEFAULT_DISPLAY_FIELDS

    if args.estimate == "compute":
        #display_fields.append("allocation_compute")
        display_fields.append("remaining_compute")
    elif args.estimate == "gpu":
        #display_fields.append("allocation_gpu")
        display_fields.append("remaining_gpu")
    elif args.estimate == "memory":
        #display_fields.append("allocation_memory")
        display_fields.append("remaining_memory")

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
    usage =Slurm.get_usage(use_sacct=args.exact, per_user=args.per_user)

    if args.output_format =='table':
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

        if args.per_user:
            for f in FIELD_PER_USER_CONFIGS:
                header_format.append(FIELD_PER_USER_CONFIGS[f].str_disp)
                header_fields.append(FIELD_PER_USER_CONFIGS[f].header)
                row_format.append(FIELD_PER_USER_CONFIGS[f].field_disp)
            header = " ".join(header_format).format(*header_fields)
            row_str = " ".join(header_format)
            row_field = " ".join(row_format)
            
            print()
            print(header)
            print('-' * len(header))
            for row in usage:
                count = 0
                percent_sum = 0.0
                sh_sum = 0.0
                for u in row['users']:
                    row_fields = []
                    for f in FIELD_PER_USER_CONFIGS:
                        row_fields.append(u[FIELD_PER_USER_CONFIGS[f].field])
                    if count > 0:
                        row_fields[0] = ""

                    percent_sum = percent_sum + u['percent_used']
                    sh_sum = sh_sum + u['sh_used']
                    
                    print(row_field.format(*row_fields))
                    count = count + 1

                if count > 0:
                    print(row_field.format("", "Total", percent_sum, sh_sum))
                    print('-' * len(header))
        else:
            for f in display_fields:
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
                topic_check = topic_fields.copy()
                topic_check.remove('')
                if len(topic_check) > 1:
                    print(" ".join(topic_format).format(*topic_fields))
            print(header)
            print('-' * len(header))
            for u in usage:
                row_fields = []
                for f in display_fields:
                    row_fields.append(u[FIELD_CONFIGS[f].field])
                
                if u['su_alloc'] == 'unlimited':
                    print(row_str.format(*row_fields))
                else:
                    print(row_field.format(*row_fields))
            print()
    elif args.output_format == 'json':
        j = json.dumps(usage)
        print(j)
