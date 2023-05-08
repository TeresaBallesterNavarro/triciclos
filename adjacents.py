from pyspark import SparkContext
import sys

def mapper(line):
    edge = line.split(',')
    n1 = edge[0][1:-1]
    n2 = edge[1][1:-1]
    return [(n1,n2), (n2,n1)]

SAMPLE = 15

sc = SparkContext()

rdd = sc.textFile(sys.argv[1])
print('textFile', rdd.take(SAMPLE))

rdd = rdd.flatMap(mapper)
print('flatMap', rdd.take(SAMPLE))

rdd = rdd.filter(lambda x: x[0]!=x[1])
print('filter', rdd.take(SAMPLE))

rdd = rdd.distinct()
print('distinct', rdd.take(SAMPLE))

rdd = rdd.groupByKey()
print('groupByKey', rdd.take(SAMPLE))

print('Result:')
for node, adjacents in rdd.collect():
    print(node, 'is adjacent to', set(adjacents), 'it has grade', len(set(adjacents))) 
