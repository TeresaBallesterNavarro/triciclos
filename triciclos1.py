from pyspark import SparkContext #Para interactuar con Spark
import sys

def mapper(line):
    edge = line.split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2
        
def generar_triciclos(listaNodos):
    """
    Toma una lista de nodos como entrada y genera triciclos a partir de la lista
    de nodos. ¿Cómo lo hace? --> Itera sobre listaNodos  y crea tuplas de la 
    forma ((nodo, n), 'exists') para cada nodo 'n'. Genera tuplas de la forma
    ((nodo, nodo_j), ('pending', nodo)) para cada par de nodos (nodo, nodo_j).
    Devuelve la lista de triciclos generados.
    """
    nodo = listaNodos[0]
    lista = listaNodos[1]
    tricilos = []
    for i in range(len(lista)): 
        
        n = lista[i]
        tricilos.append(((nodo,n),'exists'))
        
        for j in range(i+1, len(lista)):
            tricilos.append(((n,lista[j]),('pending', nodo)))
            
    return tricilos

def main(filename): 
    """
    Define la función principal main que toma un objeto
    SparkContext y un nombre de archivo como entrada. Esta función es 
    responsable de realizar el procesamiento principal del programa.
    """
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        
        grafo_rdd = sc.textFile(filename) 
        aristas_filtardas = grafo_rdd.map(mapper).distinct().filter(lambda x: x!=None)
        adyacentes = aristas_filtardas.groupByKey().map(lambda x: (x[0],sorted(list(x[1])))).sortByKey()
        triciclos = adyacentes.flatMap(generar_triciclos).groupByKey().collect()
        
        result = [] #Almacenamos los triciclos encontrados en el grafo
        for nodo, mensajes in triciclos:
            mensaje = list(mensajes) #convierte los valores de mensajes en una lista para iterar sobre ellos
            if len(mensaje)>1 and 'exists' in mensaje: 
                for i in mensaje:
                    if i !='exists': 
                        result.append((i[1],nodo[0],nodo[1])) #Añadimos los nodos que forman el triciclo
       
        print(sorted(result)) #Imprime la lista de triciclos ordenada alfabéticamente

if __name__=="__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))

    filename = sys.argv[1]
    main(filename)
          