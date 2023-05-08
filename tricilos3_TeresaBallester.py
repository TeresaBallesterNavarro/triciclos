"""
Triciclos3 : Supongamos que los datos del grafo se encuentran repartidos en
             múltiples ficheros. Queremos calcular los 3-ciclos, pero sólamente
             aquellos que sean locales a cada uno de los ficheros.
             Escribe un programa paralelo que calcule independientemente los 
             3-ciclos de cada uno de los ficheros de entrada.


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

def encontrar_triciclos(grafo):
    
   aristas_filtaradas =grafo.map(mapper).distinct().filter(lambda x: x!=None)
   adyacentes=aristas_filtaradas.groupByKey().map(lambda x: (x[0],sorted(list(x[1])))).sortByKey()
   triciclos=adyacentes.flatMap(generar_triciclos).groupByKey()
   lista_triciclos=[]
   
   for pareja, mensajes in triciclos.collect():
       mens=list(mensajes)
       if len(mens)>1 and 'exists' in mens:
           for m in mens:
               if m!='exists': 
                   lista_triciclos.append((m[1],pareja[0],pareja[1]))
   return sorted(lista_triciclos)

def main(sc, lista_filenames):
    #Sólo queremos para cada fichhero sus triciclos correspondientes
    for filename in lista_filenames:
        grafo = sc.textFile(filename)
        triciclos = encontrar_triciclos(grafo)
        print("Archivo", filename, "--> triciclos locales:", triciclos)


if __name__=="__main__":
    
    if len(sys.argv) < 2:
        print("Uso: python3 {0} <list of files>".format(sys.argv[0]))
        
    else:
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, sys.argv[1:])