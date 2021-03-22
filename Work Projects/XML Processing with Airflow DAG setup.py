# noinspection PyPep8Naming
import shutil
import logging
import os
from collections import OrderedDict
from datetime import datetime
from xml.etree.ElementTree import parse
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

log = logging.getLogger(__name__)

#external_data_inbox = os.path.join(Path(os.getenv('SFTP_HOME')). os.getenv('HOME'), 'externaldata/inbox')
#core_path = os.path.join(Path(os.getenv('SFTP_HOME')).parent, 'tsd-process-logs')

external_data_inbox = "/home/root/externaldata/inbox"
core_path = "/home/root/tsd-process-logs"
move_path = "/home/root/tsd-process-logs/archive"

def keys_exists(element, *keys):
    '''
    Check if *keys (nested) exists in `element` (dict).
    '''
    if not isinstance(element, dict):
        raise AttributeError('keys_exists() expects dict as first argument.')
    if len(keys) == 0:
        raise AttributeError('keys_exists() expects at least two arguments, one given.')

    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True


def tsd_processing(file):
    # get the XML log file and parse it
    target = file
    log_file = parse(target)
    root = log_file.getroot()

    # set up the keys analagous to the original column headers, except for sample id
    log_keys = ['input_module', 'input_time', 'desealed_time', 'CM', 'CM_input', 'CM_output', 'decapped_time', 'PNP',
                'PNP_input', 'PNP_output', 'aliquot',
                'recapped_time', '1_inst', '1_inst_presented', '2_inst', '2_inst_presented', '3_inst',
                '3_inst_presented',
                '4_inst', '4_inst_presented', '5_inst', '5_inst_presented',
                '6_inst', '6_inst_presented', 'sealed', 'output_module', 'output_time']

    log_dict = OrderedDict()
    sample_pass = {}

    # assign attribute values to variables
    for step in root.findall('ProcessStep'):
        process_dt = step.get('timestamp')
        if process_dt[-3] == ":":
            process_dt = process_dt[:-3] + process_dt[-2:]
            process_dt = datetime.strptime(process_dt, "%Y-%m-%dT%H:%M:%S.%f%z")
        else:
            process_dt = datetime.strptime(process_dt, "%Y-%m-%dT%H:%M:%S.%f%z")
        sample_id = step.get('sampleID')
        carrier_id = step.get('carrierID')
        node_id = step.get('nodeID')
        process_step = step.get('processstep')

    # need to create dictionary like sample_id: 'sample_id', module_id:[list of timestamps]
    # to check length of list for a key, use len(log_dict[key][nested key])
        if sample_id not in log_dict:
            sample_pass.update({sample_id: 1})
            log_dict[sample_id] = {'pass 1': None}
            log_dict[sample_id]['pass 1'] = dict.fromkeys(log_keys)
            if process_step == 'Processed':
                if node_id[:3] in ["FIM", "IOM", "RIM", "SRM"]:
                    # noinspection PyUnresolvedReferences
                    log_dict[sample_id]['pass 1']['input_module'] = node_id
                    log_dict[sample_id]['pass 1']['input_time'] = process_dt
        else:
            pass_count = sample_pass[sample_id]
            if process_step == 'Processed':
                if node_id[:3] in ["FIM", "IOM", "RIM", "SRM"]:
                    log_dict[sample_id].update({'pass ' + str(pass_count): None})
                    log_dict[sample_id]['pass ' + str(pass_count)] = dict.fromkeys(log_keys)
                    log_dict[sample_id]['pass ' + str(pass_count)]['input_module'] = node_id
                    log_dict[sample_id]['pass ' + str(pass_count)]['input_time'] = process_dt
                elif node_id[:3] == "DSM":
                    try:
                        log_dict[sample_id]['pass ' + str(pass_count)]['desealed_time'] = process_dt
                    except KeyError:
                        log_dict[sample_id].update({'pass ' + str(pass_count): None})
                        log_dict[sample_id]['pass ' + str(pass_count)] = dict.fromkeys(log_keys)
                        log_dict[sample_id]['pass ' + str(pass_count)]['desealed_time'] = process_dt
                elif node_id[:3] == "RCM":
                    try:
                        log_dict[sample_id]['pass ' + str(pass_count)]['recapped_time'] = process_dt
                    except KeyError:
                        log_dict[sample_id].update({'pass ' + str(pass_count): None})
                        log_dict[sample_id]['pass ' + str(pass_count)] = dict.fromkeys(log_keys)
                        log_dict[sample_id]['pass ' + str(pass_count)]['recapped_time'] = process_dt
                elif node_id[:3] == "DCM":
                    try:
                        log_dict[sample_id]['pass ' + str(pass_count)]['decapped_time'] = process_dt
                    except KeyError:
                        log_dict[sample_id].update({'pass ' + str(pass_count): None})
                        log_dict[sample_id]['pass ' + str(pass_count)] = dict.fromkeys(log_keys)
                        log_dict[sample_id]['pass ' + str(pass_count)]['decapped_time'] = process_dt
                elif node_id[:2] == "CM":
                    try:
                        log_dict[sample_id]['pass ' + str(pass_count)]['CM'] = node_id
                        log_dict[sample_id]['pass ' + str(pass_count)]['CM_output'] = process_dt
                    except KeyError:
                        log_dict[sample_id].update({'pass ' + str(pass_count): None})
                        log_dict[sample_id]['pass ' + str(pass_count)] = dict.fromkeys(log_keys)
                        log_dict[sample_id]['pass ' + str(pass_count)]['decapped_time'] = process_dt
                elif node_id[:3] in ["HIM", "STR", "IAU"]:
                    try:
                        log_dict[sample_id]['pass ' + str(pass_count)]['PNP'] = node_id
                        log_dict[sample_id]['pass ' + str(pass_count)]['PNP_output'] = process_dt
                    except KeyError:
                        log_dict[sample_id].update({'pass ' + str(pass_count): None})
                        log_dict[sample_id]['pass ' + str(pass_count)] = dict.fromkeys(log_keys)
                        log_dict[sample_id]['pass ' + str(pass_count)]['PNP'] = node_id
                        log_dict[sample_id]['pass ' + str(pass_count)]['PNP_output'] = process_dt
                elif node_id[:3] in ["C16", "ISR", "ATL", "G8", "BP2", "250", "AIA", "ICQ", "LXL"]:
                    for i in range(1, 7):
                        try:
                            if log_dict[sample_id]['pass ' + str(pass_count)][str(i) + '_inst'] is None:
                                log_dict[sample_id]['pass ' + str(pass_count)][str(i) + '_inst'] = node_id
                                log_dict[sample_id]['pass ' + str(pass_count)][str(i) + '_inst_presented'] = process_dt
                                break
                        except KeyError:
                            log_dict[sample_id].update({'pass ' + str(pass_count): None})
                            log_dict[sample_id]['pass ' + str(pass_count)] = dict.fromkeys(log_keys)
                            if log_dict[sample_id]['pass ' + str(pass_count)][str(i) + '_inst'] is None:
                                log_dict[sample_id]['pass ' + str(pass_count)][str(i) + '_inst'] = node_id
                                log_dict[sample_id]['pass ' + str(pass_count)][str(i) + '_inst_presented'] = process_dt
                                break
                elif node_id[:2] == "SM":
                    try:
                        log_dict[sample_id]['pass ' + str(pass_count)]['sealed'] = process_dt
                    except KeyError:
                        log_dict[sample_id].update({'pass ' + str(pass_count): None})
                        log_dict[sample_id]['pass ' + str(pass_count)] = dict.fromkeys(log_keys)
                        log_dict[sample_id]['pass ' + str(pass_count)]['sealed'] = process_dt
                elif node_id[:3] == "AQM":
                    try:
                        if log_dict[sample_id]['pass ' + str(pass_count)]['aliquot'] is None:
                            log_dict[sample_id]['pass ' + str(pass_count)]['aliquot'] = process_dt
                        # need to remove the daughter aliquot time. This usually occurs within 2 minutes of the previous aliquot time.
                        # timedelta function doesn't have minutes, but does have seconds.
                        elif log_dict[sample_id]['pass ' + str(pass_count)]['aliquot'] is not None and (
                                (process_dt -
                                    log_dict[
                                        sample_id][
                                        'pass ' + str(
                                            pass_count)][
                                        'aliquot']).seconds / 60) <= 2:
                            pass
                    except KeyError:
                        log_dict[sample_id].update({'pass ' + str(pass_count): None})
                        log_dict[sample_id]['pass ' + str(pass_count)] = dict.fromkeys(log_keys)
                        if log_dict[sample_id]['pass ' + str(pass_count)]['aliquot'] is None:
                            log_dict[sample_id]['pass ' + str(pass_count)]['aliquot'] = process_dt
                        # need to remove the daughter aliquot time. This usually occurs within 2 minutes of the previous aliquot time.
                        # timedelta function doesn't have minutes, but does have seconds.
                        elif log_dict[sample_id]['pass ' + str(pass_count)]['aliquot'] is not None and (
                                (process_dt -
                                    log_dict[
                                        sample_id][
                                        'pass ' + str(
                                            pass_count)][
                                        'aliquot']).seconds / 60) <= 2:
                            pass
            elif process_step == 'Unloaded':
                if node_id[:2] == "CM":
                    try:
                        log_dict[sample_id]['pass ' + str(pass_count)]['CM_input'] = process_dt
                    except KeyError:
                        log_dict[sample_id].update({'pass ' + str(pass_count): None})
                        log_dict[sample_id]['pass ' + str(pass_count)] = dict.fromkeys(log_keys)
                        log_dict[sample_id]['pass ' + str(pass_count)]['CM_input'] = process_dt
                elif node_id[:3] in ["HIM", "STR", "IAU"]:
                    try:
                        log_dict[sample_id]['pass ' + str(pass_count)]['PNP_input'] = process_dt
                    except KeyError:
                        log_dict[sample_id].update({'pass ' + str(pass_count): None})
                        log_dict[sample_id]['pass ' + str(pass_count)] = dict.fromkeys(log_keys)
                        log_dict[sample_id]['pass ' + str(pass_count)]['PNP_input'] = process_dt
                elif node_id[:3] in ["IOM", "ROM", "SRM"]:
                    if pass_count > 1 and ((process_dt - log_dict[sample_id]['pass ' + str(pass_count - 1)][
                        'output_time']).seconds / 60) <= 2:
                        pass
                    elif len(carrier_id) == 0:
                        pass
                    else:
                        try:
                            log_dict[sample_id]['pass ' + str(pass_count)]['output_module'] = node_id
                            log_dict[sample_id]['pass ' + str(pass_count)]['output_time'] = process_dt
                            sample_pass[sample_id] = sample_pass[sample_id] + 1
                        except KeyError:
                            log_dict[sample_id].update({'pass ' + str(pass_count): None})
                            log_dict[sample_id]['pass ' + str(pass_count)] = dict.fromkeys(log_keys)
                            log_dict[sample_id]['pass ' + str(pass_count)]['output_module'] = node_id
                            log_dict[sample_id]['pass ' + str(pass_count)]['output_time'] = process_dt
                            sample_pass[sample_id] = sample_pass[sample_id] + 1
                            
    log_fields = ['sample_id', 'input_module', 'input_time', 'desealed_time', 'CM', 'CM_input', 'CM_output',
                  'decapped_time', 'PNP', 'PNP_input', 'PNP_output', 'aliquot',
                  'recapped_time', '1_inst', '1_inst_presented', '2_inst', '2_inst_presented', '3_inst',
                  '3_inst_presented', '4_inst', '4_inst_presented', '5_inst', '5_inst_presented',
                  '6_inst', '6_inst_presented', 'sealed', 'output_module', 'output_time']

    # noinspection SpellCheckingInspection
    csv_headers = ['sample_id', 'input_module', 'input_time', 'desealed_time', 'CM', 'CM_input', 'CM_output',
                   'decapped_time', 'PNP', 'PNP_input', 'PNP_output', 'aliquot',
                   'recapped_time', 'inst_1', 'inst_1_presented', 'inst_2', 'inst_2_presented', 'inst_3',
                   'inst_3_presented', 'inst_4', 'inst_4_presented', 'inst_5', 'inst_5_presented',
                   'inst_6', 'inst_6_presented', 'sealed', 'output_module', 'output_time']

    time_fields = ['input_time', 'desealed_time', 'CM_input', 'CM_output', 'decapped_time', 'PNP_input', 'PNP_output',
                   'aliquot',
                   'recapped_time', '1_inst_presented', '2_inst_presented', '3_inst_presented', '4_inst_presented',
                   '5_inst_presented',
                   '6_inst_presented', 'sealed', 'output_time']

    # convert the nested dictionary to list of lists to prepare for output to csv
    # noinspection DuplicatedCode
    output_list = [csv_headers]
    time_format = '%Y-%m-%d %H:%M:%S'
    # output_list.append(log_fields)
    for k in log_dict:
        pass_num = sample_pass[k]
        for i in range(1, pass_num):
            if log_dict[k]['pass ' + str(i)]['input_module'] is None:
                pass

            else:
                pass_out = []
                sample_id = k
                pass_out.append(sample_id)
                for item in log_fields[1:]:
                    if item in time_fields:
                        if log_dict[k]['pass ' + str(i)][item] is None:
                            pass_out.append('')
                        else:
                            time_value = log_dict[k]['pass ' + str(i)][item]
                            pass_out.append(time_value.strftime(time_format))
                    else:
                        if log_dict[k]['pass ' + str(i)][item] is None:
                            pass_out.append('')
                        else:
                            pass_out.append(log_dict[k]['pass ' + str(i)][item])
                output_list.append(pass_out)

        if keys_exists(log_dict, k, 'pass ' + str(sample_pass[k])) == True:
            if log_dict[k]['pass ' + str(sample_pass[k])]['input_module'] is None:
                pass
            else:
                pass_out = []
                sample_id = k
                pass_out.append(sample_id)
                for item in log_fields[1:]:
                    if item in time_fields:
                        if log_dict[k]['pass ' + str(sample_pass[k])][item] is None:
                            pass_out.append('')
                        else:
                            time_value = log_dict[k]['pass ' + str(sample_pass[k])][item]
                            pass_out.append(time_value.strftime(time_format))
                    else:
                        if log_dict[k]['pass ' + str(sample_pass[k])][item] is None:
                            pass_out.append('')
                        else:
                            pass_out.append(log_dict[k]['pass ' + str(sample_pass[k])][item])
                output_list.append(pass_out)

    time_stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    ofn_file = file.split('/')[4]
    file_out_name = os.path.join(external_data_inbox, 'DS_TSD_PROCESS_LOGS__EA_' + time_stamp + '_OFN_' + ofn_file + '.csv')

    # output the list of lists to csv
    with open(file_out_name, 'w', newline='\n') as file_out:
        for row in output_list:
            file_out.write(','.join(str(item) for item in row))
            file_out.write('\n')


# noinspection PyPep8Naming
def process_TSD_files(**kwargs):
    try:
        if not os.path.exists(core_path):
            os.makedirs(core_path)
            os.system('chmod 777 - R {}'.format(core_path))

        if not os.path.exists(move_path):
            os.makedirs(move_path)
            os.system('chmod 777 - R {}'.format(move_path))

        # noinspection PyPep8
        files_in_dir = [os.path.join(core_path, f) for f in os.listdir(core_path) if
                        f.lower().endswith('.xml') and f.lower().startswith('process')]

        if len(files_in_dir) == 0:
            log.info('No files found to move')

        log.info('found: {} total files in staging path inbox'.format(len(files_in_dir)))

        count_processed = 0

        for file in files_in_dir:
            tsd_processing(file)
            count_processed += 1
            file_name = file.split('/')[4]
            move_name = os.path.join(move_path, file_name)
            shutil.move(file, move_name)

        log.info('Processed {} files for TSD AIP ingestion.'.format(count_processed))

    except Exception as ex:
        details = ex.args[0]
        details = details.replace("'", "")
        raise AssertionError("Process failed with error: {}".format(details))


# DAG arguments
args = {
    "start_date": datetime(2018, 1, 1),
    "owner": "airflow"
}

# Run every 5 minutes
dag = DAG(
    dag_id='tsd_full_processing',
    default_args=args,
    schedule_interval='0/5 * * * *',
    max_active_runs=1,
    catchup=False)

# task definitions
tsd_process = PythonOperator(
    task_id="process_TSD_files",
    provide_context=True,
    python_callable=process_TSD_files,
    retries=0,
    dag=dag)
