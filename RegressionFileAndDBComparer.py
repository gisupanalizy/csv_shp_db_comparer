import shapefile
import time
import csv
import os
import psycopg2
from operator import itemgetter

log_messages = []
log_messages_info = []
procs = []
procs = []
cur = None
batch_from = None
batch_to = None
total_start_time = time.time()

def shapefilereader(records_dict_list, sf, which):
    sf = shapefile.Reader(r"{0}".format(sf), encoding="windows-1252")
    log_messages_info.append('Collecting data from 1st file...')
    print('Collecting data from {0} file...'.format(which))
    for shapeRec in sf.iterShapeRecords():
        geom = shapeRec.shape.points[0]
        record_dict = shapeRec.record.as_dict()
        tpl = (geom[0], geom[1])
        record_dict['geometry'] = tpl
        records_dict_list.append(record_dict)
    return(records_dict_list, sf)

def csvreader(records_dict_list, sf, delimiter, which, group):
    with open(sf, encoding="windows-1252") as file:
        if group in ('points_11', 'points_13', 'points_15', 'points_35'):
            reader = csv.DictReader(file, delimiter=delimiter, fieldnames='1')
        elif group in ('points_14'):
            fields = []
            for n in list(range(1,25)):
                fields.append(str(n))
            reader = csv.DictReader(file, delimiter=delimiter, fieldnames=fields)
        elif group in ('points_18'):
            fields = []
            for n in list(range(1,14)):
                fields.append(str(n))
            reader = csv.DictReader(file, delimiter=delimiter, fieldnames=fields)
        else:
            reader = csv.DictReader(file, delimiter=delimiter)
        log_messages_info.append('Collecting data from 1st file...')
        print('Collecting data from {0} file...'.format(which))
        for row in reader:
            record_dict = dict(row)
            records_dict_list.append(record_dict)
    return (records_dict_list, sf)

def dbreader(cur, batch_from, batch_to, records_dict_list, tbl, which):
    log_messages_info.append('Collecting data from {0} table...'.format(which))
    print('Collecting data from {0} table...'.format(which))
    query = ('select * from '
                '(select *, row_number() OVER (order by id) as rnum FROM {0}) x '
                'where rnum between {1} and {2}'.format(tbl, batch_from, batch_to))
    cur.execute(query)
    columns = list(cur.description)
    result = cur.fetchall()
    # make dict
    for row in result:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col.name] = row[i]
        records_dict_list.append(row_dict)
    return records_dict_list

def duplicates(lst):
    seen = set()
    dups = []
    for i in sorted(lst, reverse=True):
        if i in seen:
            dups.append(i)
        seen.add(i)
    return dups

def comparisontool(cur, batch_from, batch_to, sort_fields, tbl1=None, tbl2=None, sf1=None, sf2=None, group=None, delimiter=None, omitfields=[]):

    log_messages = []
    log_messages_info = []
    start_time = time.time()
    # checking 1st file extension
    try:
        sf1_filename, sf1_file_extension = os.path.splitext(sf1)
        sf1 = sf1.replace('\\', '/')
        sf2 = sf2.replace('\\', '/')
    except:
        sf1_file_extension = None

    log_messages_info.append('Reading files of group {0} from files {1} and {2}...'.format(group, sf1, sf2))
    print('Reading files of group {0} from files {1} and {2}...'.format(group, sf1, sf2))

    records_dict_list1 = []
    records_dict_list2 = []
    # reading shapefiles (multiprocessing)
    if sf1_file_extension and sf1_file_extension.upper() == '.SHP':
        records_dict_list1, sf1 = shapefilereader(records_dict_list1, sf1, '1st')
        records_dict_list2, sf2 = shapefilereader(records_dict_list2, sf2, '2nd')
    # reading text files
    elif sf1_file_extension:
        records_dict_list1, sf1 = csvreader(records_dict_list1, sf1, delimiter, '1st', group)
        records_dict_list2, sf2 = csvreader(records_dict_list2, sf2, delimiter, '2nd', group)
    # reading db tables (multiprocessing)
    else:
        # records_dict_list1 = dbreader(records_dict_list1, tbl1, '1st')
        # records_dict_list2 = dbreader(records_dict_list2, tbl2, '2nd')
        records_dict_list1 = dbreader(cur, batch_from, batch_to, records_dict_list1, tbl1, '1st')
        records_dict_list2 = dbreader(cur, batch_from, batch_to, records_dict_list2, tbl2, '2nd')

    fields_error = 1
    if sf1_file_extension and sf1_file_extension.upper() == '.SHP':
        log_messages_info.append('Checking field types for SHP files...')
        print('Checking field types for SHP files...')
        if sf1.fields == sf2.fields:
            fields_error = 0
            log_messages.append('Types are OK')
            print('Types are OK')
        else:
            for field in sf1.fields:
                if not(any(field[0] in sublist for sublist in sf2.fields)):
                    log_messages.append('Field {0} is missing in 2nd file'.format(field))
                    print('Field {0} is missing in 2nd file'.format(field))
                elif field not in sf2.fields:
                    log_messages.append('Field {0} type is not matching'.format(field))
                    print('Field {0} type is not matching'.format(field))
            for field in sf2.fields:
                if not(any(field[0] in sublist for sublist in sf1.fields)):
                    log_messages.append('Field {0} is missing in 1st file'.format(field))
                    print('Field {0} is missing in 1st file'.format(field))
        if fields_error == 1:
            log_messages.append('Types or fields are NOT OK')
            print('Types or fields are NOT OK')
            # return(log_messages)
            
    if len(sort_fields) == 1:
        log_messages_info.append('Collecting {0} values for 1st file...'.format(sort_fields[0]))
        print('Collecting {0} values for 1st file...'.format(sort_fields[0]))
        sf1_list_of_identifiers = [i[sort_fields[0]] for i in records_dict_list1]
        log_messages_info.append('Collecting {0} values for 2nd file...'.format(sort_fields[0]))
        print('Collecting {0} values for 2nd file...'.format(sort_fields[0]))
        sf2_list_of_identifiers = [i[sort_fields[0]] for i in records_dict_list2]
    else:
        log_messages_info.append('Collecting {0} and {1} values for 1st file...'.format(sort_fields[0], sort_fields[1]))
        print('Collecting {0} and {1} values for 1st file...'.format(sort_fields[0], sort_fields[1]))
        sf1_list_of_identifiers = [str(i[sort_fields[0]])+'_' + str(i[sort_fields[1]]) for i in records_dict_list1]
        log_messages_info.append('Collecting {0} and {1} values for 2nd file...'.format(sort_fields[0], sort_fields[1]))
        print('Collecting {0} and {1} values for 2nd file...'.format(sort_fields[0], sort_fields[1]))
        sf2_list_of_identifiers = [str(i[sort_fields[0]]) + '_' + str(i[sort_fields[1]]) for i in records_dict_list2]

    removed_or_added = []
    sf1_dups = []
    sf2_dups = []
    if group not in ('points_15', 'points_35'):
        log_messages_info.append('Looking for duplicates...')
        print('Looking for duplicates...')
        sf1_dups = duplicates(sf1_list_of_identifiers)
        sf2_dups = duplicates(sf2_list_of_identifiers)
        dups1 = []
        dups2 = []
        for dup in sf1_dups:
            dups1.append(dup)
            removed_or_added.append(dup)
        for dup in sf2_dups:
            dups2.append(dup)
            removed_or_added.append(dup)
        count_dups1 = len(dups1)
        count_dups2 = len(dups2)
        try:
            id_name = sort_fields[0] + '_' + sort_fields[1]
        except:
            id_name = sort_fields[0]
        log_messages.append("DUPLICATES: {0} duplicates of column {1} found in 1st source file".format(count_dups1, id_name))
        print("DUPLICATES: {0} duplicates of column {1} found in 1st source file".format(count_dups1, id_name))

        if count_dups1 > 0:
            log_messages.append("DUPLICATED {0}: {1}".format(id_name, dups1))
            print("DUPLICATED {0}: {1}".format(id_name, dups1))
        log_messages.append("DUPLICATES: {0} duplicates of column {1} found in 2nd source file".format(count_dups2, id_name))
        print("DUPLICATES: {0} duplicates of column {1} found in 2nd source file".format(count_dups2, id_name))
        if count_dups2 > 0:
            log_messages.append("DUPLICATED {0}: {1}".format(id_name, dups2))
            print("DUPLICATED {0}: {1}".format(id_name, dups2))

    log_messages_info.append('Preparing sets...')
    print('Preparing sets...')
    sf1_list_of_identifiers = set(sf1_list_of_identifiers)
    sf2_list_of_identifiers = set(sf2_list_of_identifiers)

    log_messages_info.append('Looking for removed records...')
    print('Looking for removed records...')
    diff1 = sf1_list_of_identifiers.difference(sf2_list_of_identifiers)
    removed_ids = []
    for id in diff1:
        removed_or_added.append(id)
        try:
            id = int(id)
        except:
            id
        log_messages.append("REMOVED ID: {0}".format(id))
        #print("REMOVED ID: {0}".format(id))
        removed_ids.append(id)

    log_messages_info.append('Looking for added records...')
    print('Looking for added records...')
    diff2 = sf2_list_of_identifiers.difference(sf1_list_of_identifiers)
    added_ids = []
    for id in diff2:
        removed_or_added.append(id)
        try:
            id = int(id)
        except:
            id
        log_messages.append("ADDED ID: {0}" .format(id))
        #print("ADDED ID: {0}" .format(id))
        added_ids.append(id)

    log_messages_info.append('Preparing records to compare without added/removed...')
    print('Preparing records to compare without added/removed...')
    if len(sort_fields) == 1:
        records_dict_list1 = [{key: val for key, val in item.items() if key not in omitfields} for i, item in enumerate(records_dict_list1) if not(item[sort_fields[0]] in removed_or_added)]
        records_dict_list2 = [{key: val for key, val in item.items() if key not in omitfields} for i, item in enumerate(records_dict_list2) if not(item[sort_fields[0]] in removed_or_added)]
    else:
        records_dict_list1 = [{key: val for key, val in item.items() if key not in omitfields} for i, item in enumerate(records_dict_list1) if not(str(item[sort_fields[0]])+ '_' + str(item[sort_fields[1]]) in removed_or_added) and (str(item[sort_fields[0]]) + '_' + str(item[sort_fields[1]])) != (str(records_dict_list1[i-1][sort_fields[0]]) + '_' + str(records_dict_list1[i-1][sort_fields[1]]))]
        records_dict_list2 = [{key: val for key, val in item.items() if key not in omitfields} for i, item in enumerate(records_dict_list2) if not(str(item[sort_fields[0]])+ '_' + str(item[sort_fields[1]]) in removed_or_added) and (str(item[sort_fields[0]]) + '_' + str(item[sort_fields[1]])) != (str(records_dict_list2[i-1][sort_fields[0]]) + '_' + str(records_dict_list2[i-1][sort_fields[1]]))]

    records_dict_list1 = sorted(records_dict_list1, key=itemgetter(sort_fields[0]))
    records_dict_list2 = sorted(records_dict_list2, key=itemgetter(sort_fields[0]))

    log_messages_info.append('Detecting changes...')
    print('Detecting changes...')
    changes_dict = {}
    list_of_changes_dict = []
    for d1, d2 in zip(records_dict_list1, records_dict_list2):
        for key, value in d1.items():
            if value != d2[key]:
                changes_dict = {}
                if len(sort_fields) == 1:
                    id = d1[sort_fields[0]]
                    log_messages.append("CHG in {0}: {1}|Attribute: {2}|ValueBefore: '{3}'|ValueAfter: '{4}'".format(sort_fields[0], id, key, value, d2[key]))
                    #print("CHG in {0}: {1}|Attribute: {2}|ValueBefore: '{3}'|ValueAfter: '{4}'".format(sort_fields[0], id, key, value, d2[key]))
                    changes_dict['ID'] = id
                    changes_dict['Attribute'] = key
                    changes_dict['ValueBefore'] = value
                    changes_dict['ValueAfter'] = d2[key]
                    list_of_changes_dict.append(changes_dict)
                else:
                    id = list([d1[sort_fields[0]], d1[sort_fields[1]]])
                    log_messages.append("CHG in {0}: {1} {2}: {3}|Attribute: {4}|ValueBefore: '{5}'|ValueAfter: '{6}'".format(sort_fields[0], id[0], sort_fields[1], id[1], key, value, d2[key]))
                    #print("CHG in {0}: {1} {2}: {3}|Attribute: {4}|ValueBefore: {5}|ValueAfter: {6}|".format(sort_fields[0], id[0], sort_fields[1], id[1], key, value, d2[key]))
                    changes_dict['ID'] = id
                    changes_dict['Attribute'] = key
                    changes_dict['ValueBefore'] = value
                    changes_dict['ValueAfter'] = d2[key]
                    list_of_changes_dict.append(changes_dict)

    #log_messages_info.append("--- FINISHED IN %s seconds ---" % (time.time() - start_time))
    print("--- FINISHED IN %s seconds ---" % (time.time() - start_time))
    return(log_messages)

## TESTING PANEL:
## SELECT INPUT FILES:

## SHP FILES
# sf1 = r"D:\EXTRACT\AutomatedTests\new1.SHP"
# sf2 = r"D:\EXTRACT\AutomatedTests\new2.SHP"

## TXT FILES
sf1 = r"D:\EXTRACT\AutomatedTests\txt_file1.csv"
sf2 = r"D:\EXTRACT\AutomatedTests\txt_file2.csv"

## DB TABLES
# tbl1 = 'tbl1'
# tbl2 = 'tbl2'

## !!!RUN THE CODE!!!:

## FOR SHP FILES
# if __name__ == '__main__':
#     result_logs = comparisontool(None, None, None, ['ID'], None, None, sf1, sf2, '21', None, [])
#     sf1_filename = os.path.splitext(os.path.basename(sf1))[0]
#     sf1 = sf1.replace('\\', '/')
#     sf2_filename = os.path.splitext(os.path.basename(sf2))[0]
#     sf2 = sf2.replace('\\', '/')
#     # open the file in the write mode
#     with open('C:/GIT/extract_regression_tests/shp_results_{0}_{1}.csv'.format(sf1_filename, sf2_filename), 'w', newline='') as f:
#         # create the csv writer
#         writer = csv.writer(f)
#         # write a row to the csv file
#         for log in result_logs:
#             writer.writerow([log])
# print("--- TOTAL TIME: %s seconds ---" % (time.time() - total_start_time))

## FOR TEXT FILES
if __name__ == '__main__':
    result_logs = comparisontool(None, None, None, ['ID'], None, None, sf1, sf2, '21', ';', [])
    sf1_filename = os.path.splitext(os.path.basename(sf1))[0]
    sf1 = sf1.replace('\\', '/')
    sf2_filename = os.path.splitext(os.path.basename(sf2))[0]
    sf2 = sf2.replace('\\', '/')
    # open the file in the write mode
    with open('C:/GIT/extract_regression_tests/shp_results_{0}_{1}.csv'.format(sf1_filename, sf2_filename), 'w', newline='') as f:
        # create the csv writer
        writer = csv.writer(f)
        # write a row to the csv file
        for log in result_logs:
            writer.writerow([log])
print("--- TOTAL TIME: %s seconds ---" % (time.time() - total_start_time))

## FOR DB TABLES
# if __name__ == '__main__' and tbl1 and tbl2:
#     conn = psycopg2.connect(
#         host="hostname",
#         port=5032,
#         database="db_name",
#         user="user_name",
#         password="password")
#     cur = conn.cursor()
#     cur.execute('SELECT count(*) FROM {0}'.format(tbl1))
#     db_count = cur.fetchall()
#     db_count = db_count[0][0]
#     batches_size = 100_000 # set batch size for a single run of comparer (there will be as many source files as many batches will be found in a db table based on this number)
#     batches_nbr = round((db_count/batches_size))
#     print(db_count)
#     print(batches_nbr)
#     for batch in range(0,batches_nbr+1):
#         batch_from = batch * batches_size
#         batch_to = ((batch+1) * batches_size) - 1
#         print('Running batch number {0}, for obstacles with point_id between {1} and {2}'.format(batch, batch_from, batch_to))
#         result_logs = comparisontool(cur, batch_from, batch_to, ['point_id'], tbl1, tbl2, None, None, '21', None, ['load_id','ac_load_time','point_sys_id', 'uuid', 'insertion_time'])
#         # open the file in the write mode
#         with open('C:/GIT/extract_regression_tests/compare_py_1_wip_batches500k_new_{0}.csv'.format(batch), 'w', newline='') as f:
#             # create the csv writer
#             writer = csv.writer(f)
#             # write a row to the csv file
#             for log in result_logs:
#                 writer.writerow([log])
#     print("--- TOTAL TIME: %s seconds ---" % (time.time() - total_start_time))