"""
Triciclos2 : Datos en múltiples ficheros
            Considera que los datos, es decir, la lista de las aristas, no se 
            encuentran en un único fichero sino en muchos.
            Escribe un programa paralelo que calcule los 3-ciclos de un grafo
            que se encuentra denido en múltiples ficheros de entrada.

Apellidos: Ballester Navarro
Nombre: Teresa
"""

from pyspark import SparkContext
import sys


def mapper(line): 
    #Toma una línea de texto como entrada, divide la linea por coma, y extrae
    #dos nodos
    edge = line.split(',')
    n1 = edge[0][1:-1]
    n2 = edge[1][1:-1]
    
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2
    

def generar_triciclos(listaNodos):
    #Generamos triciclos a partir de una lista de nodos
    nodo = listaNodos[0]
    lista = listaNodos[1]
    tricilos = []
    
    for i in range(len(lista)):
        n = lista[i]
        tricilos.append(((nodo,n),'exists')) 
        for j in range(i+1, len(lista)):
            tricilos.append(((n,lista[j]),('pending', nodo)))
            
    return tricilos

def encontrar_triciclos(sc, lista_filenames): # lista_filenames = lista de nombres de archivos
   # with SparkContext() as sc:

    grafo = sc.emptyRDD()
    
    for filename in lista_filenames:
        grafo = grafo.union(sc.textFile(filename))
        
    aristas_filtradas = grafo.map(mapper).distinct().filter(lambda x: x!=None)
    adyacentes = aristas_filtradas.groupByKey().map(lambda x: (x[0],sorted(list(x[1])))).sortByKey()
    triciclos = adyacentes.flatMap(generar_triciclos).groupByKey()
    lista_triciclos=[]
    
    #Filtramos los triciclos con una arista común y generamos una lista de los triciclos
    #que vamos a devolver. Cada triciclo se alamacena en una tupla con sus 3 nodos.
    for nodo, lista in triciclos.collect():
        l = list(lista)
        if len(l)>1 and 'exists' in l:
            for i in l:
                if i != 'exists': 
                    lista_triciclos.append((i[1], nodo[0], nodo[1]))
                    
    print("Lista de triciclos", sorted(lista_triciclos)) #La imprimimos y ordenamos de menos a mayor los triciclos

def main(lista_filenames):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        encontrar_triciclos(sc, lista_filenames)


if __name__=="__main__":
    
    if len(sys.argv) < 2:
        print("Uso: python3 {0} <list of files>".format(sys.argv[0]))
        
    lista_filenames = sys.argv[1:]
    main(lista_filenames)