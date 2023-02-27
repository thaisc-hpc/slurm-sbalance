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

from io import StringIO

from .config import __version__, __author__, __license__, SACCT_BEGIN_DATE

Verbosity = type('Verbosity', (), {'INFO':1, 'WARNING':2, 'DEBUG':5, 'DEBUG2':6})

DEBUG = False

SU_FACTOR = 60

SINFO_CMD = 'sinfo'

SACCT_CLI = 'sacct'
SACCT_USAGE_FIELDS = ('jobid', 'user', 'account','qos','state','alloctres','elapsedraw','partition')
SACCT_USAGE_STATES = ('CD',     # COMPLETED
                      'F',      # FAILED
                      'TO',     # TIMEOUT
                      'CA'      # CANCEL
)

SACCTMGR_CLI = 'sacctmgr'
SACCTMGR_QOS_FIELDS = ('name','grptresmins','flags','description')
SACCTMGR_QOS_NODECAY_FLAG = 'NoDecay'
SACCTMGR_ASSOC_FIELDS = ('account','user','qos', 'defaultqos')

SCONTROL_CLI = 'scontrol'

__verbose_print = None

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

def get_slurm_qos() -> dict:
    """get_slurm_qos returns a dictionary containing QoS data."""
    qos_cmd = [SACCTMGR_CLI,'show', 'qos','-P',
               'format=' + ','.join(SACCTMGR_QOS_FIELDS)
    ]
    __verbose_print("QoS command: " + ' '.join(qos_cmd), level=Verbosity.DEBUG)

    qos_output_raw = subprocess.check_output(qos_cmd).decode('utf-8')
    __verbose_print("QoS output:\n" + qos_output_raw, level=Verbosity.DEBUG2)

    qos_csv = csv.DictReader(StringIO(qos_output_raw),delimiter='|')
    qos = dict()
    for row in qos_csv:
        if row['Flags'] == SACCTMGR_QOS_NODECAY_FLAG:
            grp_tres = row['GrpTRESMins'].split(',')
            res_dict = dict()
            for res in grp_tres:
                r = res.split('=')
                if len(r) == 2:
                    if r[0] == 'billing':
                        res_dict[r[0]] = int(r[1])
                    else:
                        res_dict[r[0]] = r[1]
            row['GrpTRESMins'] = res_dict
            qos[row['Name']] = row
    __verbose_print(qos, level=Verbosity.DEBUG2)

    return qos

def get_slurm_default_qos() -> set:
    """get_slurm_default_qos returns a set of defualt QoS visible by the user"""
    assoc_cmd = [SACCTMGR_CLI,
                   'show', 'assoc','-P',
                   'format=' + ','.join(SACCTMGR_ASSOC_FIELDS)
    ]
    __verbose_print("Assoc command: " + ' '.join(assoc_cmd), level=Verbosity.DEBUG)

    assoc_output_raw = subprocess.check_output(assoc_cmd).decode('utf-8')
    __verbose_print("Assoc output:\n" + assoc_output_raw, level=Verbosity.DEBUG2)

    assoc_csv = csv.DictReader(StringIO(assoc_output_raw),delimiter='|')

    # Assume defautl QoS is used for accounting 
    account_set = set()
    for row in assoc_csv:
        if row['Def QOS'] != '' and row['User'] != '':
            account_set.add(row['Def QOS'])
    __verbose_print("Accounts: " + ','.join(account_set), level=Verbosity.INFO)

    return account_set

def get_slurm_usage(qos_list:list) -> list:
    """get_slurm_usage returns a list of dictionaries containing usage information for each QoS in `qos_list`"""

    usage_cmd = [SCONTROL_CLI,'show', 'assoc','qos='+','.join(qos_list), '-o']
    __verbose_print("Usage command: " + ' '.join(usage_cmd), level=Verbosity.DEBUG)

    usage_output_raw = subprocess.check_output(usage_cmd).decode('utf-8')

    lines = usage_output_raw.strip().split('\n')
    __verbose_print("Usage output:", level=Verbosity.DEBUG2)
    __verbose_print(usage_output_raw, level=Verbosity.DEBUG2)

    usage_qos_idx = lines.index('QOS Records')

    # List all qos records shown from scontrol 
    usage_qos_raw = lines[usage_qos_idx+1:]

    if len(usage_qos_raw) < 1:
        # Nothing to do
        return

    usage = []
    for line in usage_qos_raw:
        record = line.split(' ')
        account_idx = record.index('Account')

        qos_configs = record[:account_idx]

        configs = [x.split('=', 1) for x in qos_configs]
        configs = {x[0]: x[1] if len(x) == 2 else '' for x in configs}

        if configs['QOS'] != '':
            qos_idx = configs['QOS'].index('(') 
            qos_name = configs['QOS'][:qos_idx]

        if configs['GrpTRESMins'] != '':
            grp_tres_min = [x.split('=') for x in configs['GrpTRESMins'].split(',')]
            grp_tres_min = {x[0]:x[1].replace(')','').split('(') for x in grp_tres_min}
            for k in grp_tres_min:
                grp_tres_min[k] = {'limit': 'N' if grp_tres_min[k][0] == 'N' else int(grp_tres_min[k][0]), 'used': int(grp_tres_min[k][1])}

        if grp_tres_min['billing']['limit'] == 'N':    
            usage.append({
                "account": qos_name,
                "su_used": grp_tres_min['billing']['used'],
                "su_limit": 'unlimited',
                "su_remaining": '-',
                "percent_used": '-',
                "percent_remaining": '-'
            })
        else:
            u = {
                "account": qos_name,
                "su_used": int(grp_tres_min['billing']['used'] / SU_FACTOR),
                "su_limit":  int(grp_tres_min['billing']['limit'] / SU_FACTOR),
                "su_remaining":  int((grp_tres_min['billing']['limit'] - grp_tres_min['billing']['used']) / SU_FACTOR),
                "su_used_raw": int(grp_tres_min['billing']['used'] / SU_FACTOR),
                "su_limit_raw":  int(grp_tres_min['billing']['limit'] / SU_FACTOR),
                "su_remaining_raw":  int((grp_tres_min['billing']['limit'] - grp_tres_min['billing']['used']) / SU_FACTOR),
                "percent_used": float(grp_tres_min['billing']['used']) / grp_tres_min['billing']['limit']
            }
            u['percent_remaining'] = 1.0 - u['percent_used']
            usage.append(u)
    
    __verbose_print(usage, level=Verbosity.DEBUG)

    return usage

def main():
    args = parse_args()  

    # Update verbose printing fuction
    if args.verbose:
        def verbose_print(*a, **k):
            if k.pop('level', 0) <= args.verbose:
                print(*a, **k)
    else:
        verbose_print = lambda *a, **k: None

    global __verbose_print
    __verbose_print = verbose_print
    __verbose_print(args, level=Verbosity.DEBUG)

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
    __verbose_print("User:     " + user, level=Verbosity.INFO)

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
