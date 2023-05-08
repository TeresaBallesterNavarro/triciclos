from pyspark import SparkContext
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
    nodo = listaNodos[0]
    lista = listaNodos[1]
    
    tricilos = []
    for i in range(len(lista)):
        n = lista[i]
        tricilos.append(((nodo,n),'exists'))
        for j in range(i+1, len(lista)):
            tricilos.append(((n,lista[j]),('pending', nodo)))
            
    return tricilos    

def calcular_triciclos(grafo):
   aristas_filtardas = grafo.map(mapper).distinct().filter(lambda x: x!=None)
   adyacentes = aristas_filtardas.groupByKey().map(lambda x: (x[0],sorted(list(x[1])))).sortByKey()
   triciclos = adyacentes.flatMap(generar_triciclos).groupByKey().collect()
   
   result = []
   for nodo, mensajes in triciclos:
       mensaje = list(mensajes)
       if len(mensaje)>= 2 and 'exists' in mensaje:
           for i in mensaje:
               if i !='exists': 
                   result.append((i[1],nodo[0],nodo[1]))
                   
   return sorted(result) #Devolvemos los triciclos del grafo ordenados alfabéticamente

def main(filenames):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        
        for filename in filenames:
            grafo_rdd = sc.textFile(filename)
            triciclos = calcular_triciclos(grafo_rdd)
            print("Los tricilos de", filename, "son:", triciclos)

if __name__=="__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 {0} <list of files>".format(sys.argv[0]))
     
    filenames = sys.argv[1:]
    main(filenames)