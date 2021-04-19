import argparse

parser = argparse.ArgumentParser(description='Calculates block and index metrics')
parser.add_argument('store_file', type=str)
args = parser.parse_args()

log_file = open(args.store_file, 'r')

blocks=0
index_size=0
last_line=''
for line in log_file:
    blocks = blocks + 1
    last_line = line

blocks = blocks - 1
index_size = len(last_line.split(sep=',')) // 2
log_file.close()

print('Total blocks: %d, index items: %d' % (blocks, int(index_size)))
