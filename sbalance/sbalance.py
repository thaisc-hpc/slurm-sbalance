#!/usr/bin/env python
# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
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

import numpy as np
import pandas as pd

from .config import __version__, __author__, __license__, SACCT_BEGIN_DATE

Verbosity = type('Verbosity', (), {'INFO':1, 'WARNING':2, 'DEBUG':5})

DEBUG = False

SACCT_COMMAND = 'sacct'
SACCT_USAGE_FIELDS = ('jobid', 'user', 'account','qos','state','alloctres','elapsedraw','partition')
SACCT_USAGE_STATES = ('CD',     # COMPLETED
                      'F',      # FAILED
                      'TO',     # TIMEOUT
                      'CA'      # CANCEL
)

SACCTMGR_COMMAND = 'sacctmgr'
SACCTMGR_QOS_FIELDS = ('name','grptresmins','flags','description')
SACCTMGR_QOS_NODECAY_FLAG = 'NoDecay'
SACCTMGR_ASSOC_FIELDS = ('account','user','qos', 'defaultqos')

__verbose_print = None

def query_qos():
    qos_info = {}
    qos_cmd = [SACCTMGR_COMMAND,
               '--noheader','show', 'qos','-P',
               'format=' + ','.join(SACCTMGR_QOS_FIELDS)
    ]

    __verbose_print('[SLURM]: ' + ' '.join(qos_cmd), level=Verbosity.INFO)

    # Query all available QoS
    qos_output_raw = subprocess.check_output(qos_cmd, universal_newlines=True).splitlines()
    qos_output = csv.DictReader(qos_output_raw, fieldnames=SACCTMGR_QOS_FIELDS, delimiter='|')

    for qos in qos_output:
        # Check only QoS with NoDecay flags
        if SACCTMGR_QOS_NODECAY_FLAG in qos['flags'].split(','):
            qos_info[qos['name']] = qos
            qos_info[qos['name']]['grptresmins'] = {k:int(v) for k,v in (x.split('=') for x in qos['grptresmins'].strip().split(','))}

    __verbose_print(qos_info, level=Verbosity.DEBUG)

    return qos_info

def query_assocs(qos_info, user_list=None, verbose=0):
    assoc_info = OrderedDict()
    assoc_cmd = [SACCTMGR_COMMAND,
                   '--noheader', 'show', 'assoc','-P',
                   'format=' + ','.join(SACCTMGR_ASSOC_FIELDS)
    ]

    __verbose_print('[SLURM]: ' +' '.join(assoc_cmd), level=Verbosity.INFO)

    # Assume user only have one QoS
    assoc_output_raw = subprocess.check_output(assoc_cmd, universal_newlines=True).splitlines()
    assoc_output = csv.DictReader(assoc_output_raw, fieldnames=SACCTMGR_ASSOC_FIELDS, delimiter='|')
    
    for assoc in assoc_output:
        if not assoc['user']:
            continue

        if not user_list or assoc['user'] in user_list:
            qos_list = assoc['qos'].split(',')

            for qos in qos_list:
                if qos in qos_info:
                    assoc_info[(assoc['account'], qos)] = qos_info[qos]
                else:
                    assoc_info[(assoc['account'], qos)] = {}
                
                # Has default QoS
                if len(qos_list) > 1:
                    assoc_info[(assoc['account'], qos)]['default'] = (qos == assoc['defaultqos'])
                else:
                    assoc_info[(assoc['account'], qos)]['default'] = None

    __verbose_print(assoc_info, level=Verbosity.DEBUG)

    return assoc_info

def query_usage(assoc_list=None, verbose=0, start=SACCT_BEGIN_DATE):
    usage_info = {}
    usage_cmd = [SACCT_COMMAND,
                 '-aPX', '--noheader', '--noconvert',
                 '--format=' + ','.join(SACCT_USAGE_FIELDS),
                 #'--state=' + ','.join(SACCT_USAGE_STATES),    # Looking for completed, failed, and timed out jobs.
                 '--start=' + start                             # Start from the begining of service 
    ]
    if assoc_list:
        usage_cmd.append('-q')
        usage_cmd.append(','.join([x[1] for x in assoc_list.keys()]))

    
    __verbose_print('[SLURM]: ' +' '.join(usage_cmd), level=Verbosity.INFO)

    usage_output_raw = subprocess.check_output(usage_cmd, universal_newlines=True).splitlines()
    usage_output = csv.DictReader(usage_output_raw, fieldnames=SACCT_USAGE_FIELDS, delimiter='|')

    for usage in usage_output:
        account_qos = (usage['account'], usage['qos'])

        # Skip if AllocTres is not presented
        if not usage['alloctres']:
            continue

        alloc_tres = {k:int(v.replace('M','')) for k,v in (x.split('=') for x in usage['alloctres'].strip().split(','))}
        
        #Convert seconds to minute
        elapsed_mins = int(usage['elapsedraw']) / 60.0

        if usage_info.get(account_qos, None):
            for tres in alloc_tres:
                if usage_info[account_qos].get(tres, None):
                    usage_info[account_qos][tres] += alloc_tres[tres] * elapsed_mins
                else:
                    usage_info[account_qos][tres] = alloc_tres[tres] * elapsed_mins
            usage_info[account_qos]['elaspsed_mins'] += elapsed_mins
        else:
            usage_info[account_qos] = {}
            for tres in alloc_tres:
                usage_info[account_qos][tres] = alloc_tres[tres] * elapsed_mins
            usage_info[account_qos]['elaspsed_mins'] = elapsed_mins

    
    __verbose_print(usage_info, level=Verbosity.DEBUG)

    return usage_info

def generate_user_balances(user, user_account, user_usage, units='', ignore_root=False):
    if units == 'k':
        su_units = 'kSU'
        su_factor = 1.0e3
    elif units == 'M':
        su_units = 'MSU'
        su_factor = 1.0e6
    else:
        su_units = 'SU'
        su_factor = 1

    output = []

    for assoc in user_account:
        
        account = assoc[0]
        qos = assoc[1]

        if ignore_root and account == 'root':
            continue

        balance = None
        limits = None
        usage = None    

        if user_account[assoc].get('grptresmins', None):
            limits = user_account[assoc]['grptresmins']['billing'] / su_factor
            
            if user_usage.get(assoc, None):
                usage = math.ceil(user_usage[assoc]['billing']) / su_factor
            else:
                usage = 0.0
            
            balance = limits - usage
            balance_percent = balance * 100.0 / limits

        account = {
            'account': account,
            'qos': qos,
            'description': user_account[assoc].get('description', ''),
            'unit': su_units,
            'allocation': limits if limits != None else 'unlimited',
            'remaining': balance if balance != None else '',
            'remaining_percent': balance_percent if balance != None else '',
            'used': usage if usage != None else ''
        }

        yield(account)

def print_user_balance_csv(user, user_account, user_usage, units='', col_width=15, ignore_root=False, delimiter=','):

    table_format = "{:<{col_width}s} {:<{col_width}s} {:{col_width}s} {:>{col_width}s} {:>{col_width}s} {:>{col_width}s} {:>{col_width}s}".replace('{col_width}', str(col_width))

    print(delimiter.join(['Account', 'QoS', 'Description', 'Allocation({})'.format(units+'SU'), 'Remaining({})'.format(units+'SU'), 'Remaining(%)', 'Used({})'.format(units+'SU')])) 
    for bal in generate_user_balances(user, user_account, user_usage, units, ignore_root):
        print(delimiter.join([bal['account'],
                              bal['qos'], 
                              bal['description'], 
                              str(bal['allocation']),
                              str(bal['remaining']), 
                              str(bal['remaining_percent']), 
                              str(bal['used'])
                             ]))

def print_user_balance_table(user, user_account, user_usage, units='', col_width=15, ignore_root=False):

    table_format = "{:<{col_width}s} {:<{col_width}s} {:{col_width}s} {:>{col_width}s} {:>{col_width}s} {:>{col_width}s} {:>{col_width}s}".replace('{col_width}', str(col_width))

    print("Account balances for user: %s" % user)
    print()
    print(table_format.format('Account', 'QoS', 'Description', 'Allocation({})'.format(units+'SU'), 'Remaining({})'.format(units+'SU'), 'Remaining(%)', 'Used({})'.format(units+'SU'), col_width=col_width)) 
    print(('-'*col_width + ' ') * 7 )
    
    for bal in generate_user_balances(user, user_account, user_usage, units, ignore_root):
        print(table_format.format(bal['account'],
                                  bal['qos'], 
                                  bal['description'], 
                                  "{:{col_width}.2f}".format(bal['allocation'], col_width=col_width) if isinstance(bal['allocation'], float) else str(bal['allocation']),
                                  "{:{col_width}.2f}".format(bal['remaining'], col_width=col_width) if isinstance(bal['remaining'], float) else str(bal['remaining']), 
                                  "{:{col_width}.2f}".format(bal['remaining_percent'], col_width=col_width) if isinstance(bal['remaining_percent'], float) else str(bal['remaining_percent']), 
                                  "{:{col_width}.2f}".format(bal['used'], col_width=col_width) if isinstance(bal['used'], float) else str(bal['used'])
                                  ))
    print()

def print_user_balance(user, user_account, user_usage, units=''):

    print("Account balances for user: %s" % user)

    for bal in generate_user_balances(user, user_account, user_usage, units):
        if bal['account'] == bal['qos']:
            print('Account: {}'.format(bal['account']))
        else:
            print('Account: {}'.format(bal['account']))
            print('QoS: {}'.format(bal['qos']))
            
        if bal['allocation'] != 'unlimited':
            print("\t{:20} {:>18s}".format("Description:", bal['description']))    
            print("\t{:20} {:15.2f} {}".format("Allocation:", bal['allocation'], units+'SU'))
            print("\t{:20} {:15.2f} {} ({:6.2f}%)".format("Remaining Balance:", bal['remaining'], units+'SU', bal['remaining_percent']))
            print("\t{:20} {:15.2f} {}".format("Used:", bal['used'], units+'SU'))
        else:
            print("\t{:20} {:>18}".format("Allocation:", "unlimited"))

def parse_args():
    slurm_version = str(subprocess.check_output(['sshare', '--version']))

    parser = argparse.ArgumentParser(prog='sbalance', description='Query slurm account balance.')
    version = "sbalance " + __version__ + " with " + slurm_version
    parser.add_argument(
        '-V', '--version', action='version', version=version)
    parser.add_argument(
        '-k', action='store_const', dest='unit', default='', const='k', help="show output in kSU (1,000 SU)")
    parser.add_argument(
        '-m', action='store_const', dest='unit', const='M', help="show output in MSU (1,000,000 SU)")
    parser.add_argument(
        '-d','--detail', action='store_true', help="display SU usage per users")
    parser.add_argument(
        '-c', '--csv', action='store_const', dest='pformat', const='csv', help="print output as csv")
    parser.add_argument(
        '-t', '--table', action='store_const', dest='pformat', const='table', help="print output as table")
    parser.add_argument(
        '-j', '--json', action='store_const', dest='pformat', const='json', help="print output as json")
    parser.add_argument(
        '--start', action='store', default=SACCT_BEGIN_DATE, help="starting date")
    parser.add_argument(
        '-v', '--verbose', action='count', help="verbose mode (multiple -v's increase verbosity)")

    return parser.parse_args()

def main():
    args = parse_args()  

    if args.verbose:
        def verbose_print(*a, **k):
            if k.pop('level', 0) <= args.verbose:
                pprint(*a, **k)
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
    
    qos_cmd = [SACCTMGR_COMMAND,'show', 'qos','-P',
               'format=' + ','.join(SACCTMGR_QOS_FIELDS)
    ]
    qos_output_raw = subprocess.check_output(qos_cmd).decode('utf-8')
    qos = pd.read_csv(StringIO(qos_output_raw), sep='|')
    qos['GrpTRESMins'] = qos['GrpTRESMins'].apply(lambda tres: {k:int(v) for k,v in (x.split('=') for x in tres.strip().split(','))} if not pd.isnull(tres) else tres)
    qos['Allocation'] = qos['GrpTRESMins'].apply(lambda x: x['billing'] if not pd.isnull(x) else 'unlimited')
    
    # Remove non-accountable QoS
    qos = qos.loc[qos['Flags'] == 'NoDecay']

    assoc_cmd = [SACCTMGR_COMMAND,
                   'show', 'assoc','-P',
                   'format=' + ','.join(SACCTMGR_ASSOC_FIELDS)
    ]
    # Assume user only have one QoS
    assoc_output_raw = subprocess.check_output(assoc_cmd).decode('utf-8')
    assoc = pd.read_csv(StringIO(assoc_output_raw), sep='|')
    valid_account = assoc.loc[assoc['User'].notnull()]['QOS'].unique()

    usage_cmd = [SACCT_COMMAND,
                 '-aPX', '--noconvert',
                 '--format=' + ','.join(SACCT_USAGE_FIELDS),
                 '--start=' + args.start                             # Start from the begining of service 
    ]
    usage_cmd.append('-q')
    usage_cmd.append(','.join(valid_account))

    usage_output_raw = subprocess.check_output(usage_cmd).decode('utf-8')
    usage = pd.read_csv(StringIO(usage_output_raw), sep='|')
    usage['AllocTRES'] = usage['AllocTRES'].apply(lambda tres: {k:int(v.replace('M','')) for k,v in (x.split('=') for x in tres.strip().split(','))} if not pd.isnull(tres) else tres)
    usage['ElapsedRaw'] = usage['ElapsedRaw'].apply(lambda x: x / 60.0 if not pd.isnull(x) else x)
    usage['Billing'] = usage.apply(lambda r: r['AllocTRES'].get(u'billing', 0) * r['ElapsedRaw'] if not pd.isnull(r['AllocTRES']) else 0, axis=1)
    
    account_usage = usage.groupby(['Account'], as_index=False).agg({'Billing':'sum'})
    
    if args.detail:
        account_usage = usage.groupby(['Account', 'User'], as_index=False).agg({'Billing':'sum'})
        
    qos = qos.rename(columns={'Name': 'Account'})

    result = pd.merge(qos, account_usage, on='Account', how='outer', sort=True)
    result = result.drop(columns=['GrpTRESMins', 'Flags'])
    result = result.rename(columns={'Billing': 'Used', 'Descr': 'Description'})
    
    if args.detail:
        result['User'] = result['User'].fillna('')

    result['Used'.format(su_units)] = result['Used'].apply(lambda x: 0 if pd.isnull(x) else int(math.ceil(x)))
    result['Used({})'.format(su_units)] = result['Used'].apply(lambda x: 0 if pd.isnull(x) else int(math.ceil(x)) * su_factor)
    result['Allocation({})'.format(su_units)] = result['Allocation'].apply(lambda x: 0 if pd.isnull(x) else x * su_factor)
    result['Remaining'] = result.apply(lambda r: (r['Allocation'] - r['Used']) if not type(r['Allocation']) is str else '' ,axis=1)
    result['Remaining({})'.format(su_units)] = result['Remaining'].apply(lambda x: 0 if pd.isnull(x) else x * su_factor)
    result['Remaining(%)'] = result.apply(lambda r: float(r['Remaining'])/r['Allocation']*100.0 if not type(r['Allocation']) is str else '' ,axis=1)
    result = result.loc[result['Account'].isin(valid_account)]

    pd.set_option('display.max_rows', None)
    pd.options.display.float_format = '{:.2f}'.format
    
    print()
    if args.detail:
        result['Used(%)'] = result.apply(lambda r: float(r['Used'])/r['Allocation']*100  if not type(r['Allocation']) is str else '' ,axis=1)
        table = pd.pivot_table(result, values=['Used({})'.format(su_units), 'Used(%)'], index=['Account', 'User'])

        lines = table.to_string(col_space=12, formatters={'Used(%)':'{:.2f}'.format}).split('\n')
        lines.insert(2, '-'*len(lines[0]))
        print('\n'.join(lines))
    else:
        lines = result.to_string(index=False, columns=['Account', 'Description', 'Allocation({})'.format(su_units), 'Remaining({})'.format(su_units), 'Remaining(%)', 'Used({})'.format(su_units)], col_space=12).split('\n')
        lines.insert(1, '-'*len(lines[0]))
        print('\n'.join(lines))

    #if args.pformat == 'table':
    #    print_user_balance_table(user, user_assocs, user_usage, args.unit)
    #elif args.pformat == 'csv':
    #    print_user_balance_csv(user, user_assocs, user_usage, args.unit, ignore_root=args.ignore_root)
    #elif args.pformat == 'json':
    #    print(json.dumps(list(generate_user_balances(user, user_assocs, user_usage, args.unit, args.ignore_root)), indent=4, sort_keys=True))
    #else:
    #    print_user_balance(user, user_assocs, user_usage, args.unit)