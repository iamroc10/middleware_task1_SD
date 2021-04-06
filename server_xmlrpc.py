import logging
import os
import redis
import time
import sys
from redis.client import Redis
from xmlrpc.server import SimpleXMLRPCServer
from multiprocessing import Process

WORKERS = {}
WORKER_ID = 0
llista = {}
cua = 'cua_jobs' 
r = None


    # Expose the functions 

# Funció que executa cada worker que es crea. Espera a la cua de Redis fins que té un job a fer. En tenir-ne un el tracta.
def startWorker(w_id):
    global cua
    global r
    
    while True:
        elem = r.lpop(cua)

        if elem != None:
            line = str(elem).split(':')
            task = line[0]
            task = task[2:]
        
            fitxer = line[1]
            fitxer = fitxer[:len(fitxer)]
            
            cont = line[2]
            cont = cont[2:len(cont)-1]

            num = line[3]
            num =  num[:len(num)-1]

            url = 'http://localhost:8000/' + fitxer
            os.system('curl ' + url + '> redir_'+ cont + num + '.txt')
            arg1 = 'redir_' + cont + num + '.txt'
            
            result = eval(task)(arg1)
            if (int(num) > 0):
                time.sleep(1)

                aux = r.get(cont)
                aux = str(aux)
                aux = aux[2:len(aux)-1]  
                
                result = aux + '*' + str(result)

            r.mset({cont : str(result)})
            os.system('rm ' + arg1)

    return True


# Funció en que li passem per paràmetre quina job ha de fer i sobre quins fitxers. Aquest job i fitxer/s organitza el job i l'afegeix a la cua de Redis. Retorna el job_id.
def tractamentCua(task, files):
    global cua
    global r
    i=0
    files = files.split('*')

    while (i < len(files)):
        if (i < 1):
            r.incr("counter")
            r.set(r.get("counter"), 0)
        arg = str(task) + ':' + str(files[i]) + ':' + str(r.get("counter")) + ':' + str(i)
        r.rpush(cua, arg)
        i = i + 1

    return (r.get("counter"))


# Funció que crea un procés i crida a la funció startWorker().
def createWorker():
    global WORKERS
    global WORKER_ID

    proc = Process(target=startWorker, args=(WORKER_ID,))
    proc.start()

    WORKERS[WORKER_ID] = proc
    WORKER_ID =  WORKER_ID+1

    return ('CREATED WORKER = ',WORKER_ID)


# Funció que elimina el worker amb ID passat per paràmetre. 
def deleteWorker(cont):
    global WORKERS
    global WORKER_ID

    proc1 = WORKERS[cont]
    proc1.terminate()
    proc1.is_alive()
    WORKERS.pop(cont-1)

    return ('DELETED WORKER= ',cont)


# Funció que llista els workers que hi ha actius en aquell moment.
def listWorker():
    global WORKERS
    global WORKER_ID

    newList = {}
    for i in WORKERS:
        prova = str(WORKERS[i])
        prova1 = prova.find(",")
        newList[i] = '<(WORKER= {}, {}'.format(i+1, str(prova[prova1+1:]))

    return ('LIST WORKERS= ',str(newList))


# Funció que compta el nombre de paraules que hi ha en el fitxer passat per paràmetre.
def countingWords(fitxer):
    f = open(fitxer, "r")
    line = f.read()
    num = len(line.split())

    return (num)


# Funció que compta el nombre de repeticions de cada paraula d'un fitxer passat per paràmetre.
def wordCount(fitxer):
    f = open(fitxer, "r")
    line = f.read()
    line = line.lower()
    
    counts = dict()
    words = line.split()

    for word in words:
        if word in counts:
            counts[word] = counts[word] + 1
        else:
            counts[word] = 1
    
    return (counts)


# Funció que es crida només amb la funció wordCount i que tracta les paraules repetides.
def tractamentLlista(llista):
    llistaAux = []
    i = 0
    trobat = False
    
    while (i < len(llista)):
        aux = llista[i]
        key = aux[0]
        valor = aux[1]
        y = 0
        trobat = False
        while ((y < len(llistaAux)) and trobat == False):
            if (key == llistaAux[y][0]):
                trobat = True
            y = y + 1

        if (trobat == False):
            llistaAux.append(aux)
            
        else:
            aux1 = llistaAux[y-1]
            valor1 = aux1[1]
            valor1 = int(valor1) + int(valor)
            llistaAux[y-1][1] = valor1

        i = i + 1
    
    return llistaAux

    
# Funció que retorna el resultat del job passat per paràmetre.
def getResult(job_id, task):
    global r
    llista = []

    result = r.get(job_id)
    r.delete(job_id)
    result = str(result)
    result = result[2:len(result)-1]
    i = 0
    suma = 0

    if (result.find("*") > 0):
        if (task == 'countingWords'):
            line = result.split('*')

            while (i < len(line)):
                suma = int(line[i]) + suma
                i = i + 1
            result = suma

        elif (task == 'wordCount'):
            result =  result[1:len(result)-1]            
            result = result.replace('}*{', ', ')

            line = result.split(',')
            i = 0
            while (i < len(line)):
                line1 = str(line[i])
                aux = line1.split(':')
                key = aux[0]

                if (i == 0):
                    key = key[1:len(key)-1]
                else:
                    key = key[2:len(key)-1]

                value = aux[1]
                value =  value[1:]

                llista.append([key, value])
                i = i + 1
            
            i = 0
            
            result = tractamentLlista(llista)

    return (result)


 #MAIN
if __name__ == '__main__':
        
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    server = SimpleXMLRPCServer(
       ('localhost', 9000),
        logRequests=True,
       )

    server.register_function(createWorker)
    server.register_function(deleteWorker)
    server.register_function(listWorker)
    server.register_function(countingWords)
    server.register_function(wordCount)
    server.register_function(tractamentCua)
    server.register_function(getResult)
    
    r = redis.Redis()
    r.set("counter", 0)

    # Start the server
    try:
        print('Control+C --> EXIT')
        server.serve_forever()
    except KeyboardInterrupt:
        print('Exiting')
