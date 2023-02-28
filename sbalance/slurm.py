import subprocess
import csv

from io import StringIO
from .utils import VerboseLog, Verbosity

DEBUG = False

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

SU_UNLIMITED = 'unlimited'

SU_COMPUTE_NODE_FACTOR = 128 * 1 * 60
SU_GPU_NODE_FACTOR = 4 * 112  * 60
SU_MEMORY_NODE_FACTOR = 128 * 4.5 * 60

class Slurm:

    @classmethod
    def get_qos(cls) -> dict:
        """get_slurm_qos returns a dictionary containing QoS data."""
        qos_cmd = [SACCTMGR_CLI,'show', 'qos','-P',
                'format=' + ','.join(SACCTMGR_QOS_FIELDS)
        ]
        VerboseLog.print("Get QoS command: " + ' '.join(qos_cmd), level=Verbosity.DEBUG)

        qos_output_raw = subprocess.check_output(qos_cmd).decode('utf-8')
        VerboseLog.print("QoS output:\n" + qos_output_raw, level=Verbosity.DEBUG2)

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
        VerboseLog.print(qos, level=Verbosity.DEBUG2)

        return qos

    @classmethod
    def get_default_qos(cls) -> set:
        """get_slurm_default_qos returns a set of defualt QoS visible by the user"""
        assoc_cmd = [SACCTMGR_CLI,
                    'show', 'assoc','-P',
                    'format=' + ','.join(SACCTMGR_ASSOC_FIELDS)
        ]
        VerboseLog.print("Get Association command: " + ' '.join(assoc_cmd), level=Verbosity.DEBUG)

        assoc_output_raw = subprocess.check_output(assoc_cmd).decode('utf-8')
        VerboseLog.print("Association output:\n" + assoc_output_raw, level=Verbosity.DEBUG2)

        assoc_csv = csv.DictReader(StringIO(assoc_output_raw),delimiter='|')

        # Assume defautl QoS is used for accounting 
        account_set = set()
        for row in assoc_csv:
            if row['Def QOS'] != '' and row['User'] != '':
                account_set.add(row['Def QOS'])
        return account_set

    @classmethod
    def get_usage(cls) -> list:
        """get_slurm_usage returns a list of dictionaries containing usage information for each QoS in `qos_list`"""

        qos = cls.get_qos()

        # List user accounts and associations
        qos_list = cls.get_default_qos()
        VerboseLog.print("Accounts: " + ','.join(qos_list), level=Verbosity.INFO)

        usage_cmd = [SCONTROL_CLI,'show', 'assoc','qos='+','.join(qos_list), '-o']
        VerboseLog.print("Get Usage command: " + ' '.join(usage_cmd), level=Verbosity.DEBUG)

        usage_output_raw = subprocess.check_output(usage_cmd).decode('utf-8')

        lines = usage_output_raw.strip().split('\n')
        VerboseLog.print("Usage output:", level=Verbosity.DEBUG2)
        VerboseLog.print(usage_output_raw, level=Verbosity.DEBUG2)

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

            qos_usage = {
                "account": qos_name,
                "description": qos[qos_name]['Descr'],
                "su_used_raw": grp_tres_min['billing']['used']
            }

            if grp_tres_min['billing']['limit'] == 'N':    
                qos_usage["su_alloc_raw"] = SU_UNLIMITED
                qos_usage["su_remaining_raw"] = '-'
                qos_usage["percent_used"] = '-'
                qos_usage["percent_remaining"] = '-'
            else:
                qos_usage["su_alloc_raw"] = int(grp_tres_min['billing']['limit']) 
                qos_usage["su_remaining_raw"] = int((grp_tres_min['billing']['limit'] - grp_tres_min['billing']['used']))
                qos_usage["percent_used"] = float(grp_tres_min['billing']['used']) / grp_tres_min['billing']['limit']
                qos_usage['percent_remaining'] = 1.0 - qos_usage['percent_used']
            
            usage.append(qos_usage)
        
        usage.sort(key=lambda x: x["account"])

        VerboseLog.print("Usage:", level=Verbosity.DEBUG)
        VerboseLog.print(usage, level=Verbosity.DEBUG)

        return usage

