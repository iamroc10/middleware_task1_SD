import xmlrpc.client
import time

proxy = xmlrpc.client.ServerProxy('http://localhost:9000')


def switch(num):
    result = 0
    if (num == 1): result = proxy.createWorker()
    if (num == 2): result = proxy.listWorker()
    if (num == 3): 
        quin = int(input("ID worker a eliminar (l'id comença per 1):"))
        result = proxy.deleteWorker(quin)
    if ((num == 4) or (num == 5)):
        fitxers = int(input("Quants fitxers a tractar? "))
        i = 0
        fitxer = ''
        while ((i < fitxers) and (fitxers != 0)):
            nom_fitxer = input("Nom fitxer: ")
            if ((fitxers > 1) and (i < fitxers-1)):
                fitxer = str(fitxer) + str(nom_fitxer) + '*'
            if (i == fitxers-1):
                fitxer = str(fitxer) + str(nom_fitxer)
            i = i + 1
        if (num == 4):
            task = 'wordCount'
        else: task = 'countingWords'

        job_id = proxy.tractamentCua(task, fitxer)
        time.sleep(5)
        print('\nRESULTAT '+ task + ': ')
        result = proxy.getResult(str(job_id), task)

    if (num == 6): result = -1
    
    return result


def inicialitzacio():
    print('\n1. Create Worker')
    print('2. List Workers')
    print('3. Delete Worker')
    print('4. Word Count')
    print('5. Counting Words')
    print('6. EXIT\n')


# MAIN
cond = 7
print('CREACIÓ WORKER AUTOMÀTIC')
print(proxy.createWorker())
while (cond != -1):
    inicialitzacio()
    case = int(input("Escull una opció: "))
    cond = switch(case)
    print(cond)
