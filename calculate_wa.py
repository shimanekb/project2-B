import argparse
import re

def count_entries(line : str) -> int:
    sline = line.split()
    return int(sline[8][:-1])

def count_writes(line : str) -> int:
    sline = line.split()
    return int(sline[8][:-1])

parser = argparse.ArgumentParser(description='Calculates WA')
parser.add_argument('log_file', type=str)
args = parser.parse_args()

log_file = open(args.log_file, 'r')

writes = 0
entries = 0
for line in log_file:
    if re.search('^.*Number of writes.*', line):
        entries = entries + count_entries(line)
    elif re.search('^.*Number of new.*', line):
        writes = writes + count_writes(line)

log_file.close()

wa = writes / entries
print('Total Writes: %d, Entries: %d, Wa: %.2f' % (writes, entries, wa))
