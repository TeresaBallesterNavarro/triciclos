"""
Triciclos1 :
    Los triciclos, o más formalmente 3-ciclos, son un elemento básico en el 
    análisis de grafos y subgrafos. Los 3-ciclos muestran una relación estrecha
    entre 3 entidades (vértices): cada uno de los vértices está relacionado 
    (tiene aristas) con los otros dos.
    Escribe un programa paralelo que calcule los 3-ciclos de un grafo denido como
    lista de aristas.

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

def encontrar_triciclos(sc, filename): #Funcion principal        
    grafo_rdd = sc.textFile(filename) 
    # Filtramos las aristas para que no haya elementos None
    aristas_filtradas = grafo_rdd.map(mapper).filter(lambda x : x is not None)
    # Agrupa los elementos de aristas_filtradas por clave y crea pares  
    # (nodo-lista de nodos adyacentes). 
    adyacentes = aristas_filtradas.groupByKey().map(lambda x: (x[0],sorted(list(x[1])))).sortByKey()       
    # Generamos los triciclos
    triciclos = adyacentes.flatMap(generar_triciclos).groupByKey().collect()
    
    # Una vez generados, falta almacenarlos y devolverlos en orden alfabético
    result = []
    for nodo, lista in triciclos: 
        lista = list(lista)
        if len(lista)>1 and 'exists' in lista:
            for mensaje in lista:
                if mensaje != 'exists':
                    result.append(mensaje[1], nodo[0], nodo[1])
                    
    print(sorted(result))
  

def main(filename):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        encontrar_triciclos(sc, filename)
        
if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))

    filename = sys.argv[1]
    main(filename)

