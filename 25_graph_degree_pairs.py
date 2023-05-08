# -*- coding: utf-8 -*-

from pyspark import SparkContext
import sys

def mapper(line):
    edge = line.split(',')
    n1 = edge[0][1:-1]
    n2 = edge[1][1:-1]
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2

def main(sc, filename):
    graph = sc.textFile(filename)
    graph_clean = graph.map(mapper).distinct().filter(lambda x: x != None)
    print (graph_clean.take(10))
    degree = graph_clean.flatMap(lambda x: (x[0],x[1])).countByKey()
    print ('RESULTS------------------')
    print ('graph degree', degree)
    graph_degree = graph_clean.map(lambda x: (x,(degree[x[0]],degree[x[1]])))
    print (graph_degree.take(10))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, sys.argv[1])
