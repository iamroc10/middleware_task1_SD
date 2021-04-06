import xmlrpc.client
import time

proxy = xmlrpc.client.ServerProxy('http://localhost:9000')

#creació de 3 WORKERS
print(proxy.createWorker())
print(proxy.createWorker())
print(proxy.createWorker())

#LIST WORKER
print(proxy.listWorker())

#DELETE WORKER ( 1 )
print(proxy.deleteWorker(1))

#LIST WORKER
print(proxy.listWorker())

#WORDCOUNT amb 1 fitxer
job_id = proxy.tractamentCua('wordCount', 'fitxerText.txt')
time.sleep(2)
print('\nResultat WORDCOUNT amb 1 fitxer:')
print(proxy.getResult(str(job_id), 'wordCount'))

#WORDCOUNT amb 1 fitxer
job_id = proxy.tractamentCua('wordCount', 'fitxerText2.txt')
time.sleep(2)
print('\nResultat WORDCOUNT amb 1 fitxer:')
print(proxy.getResult(str(job_id), 'wordCount'))

#COUNTINGWORDS  amb 1 fitxer
job_id = proxy.tractamentCua('countingWords', 'fitxerText.txt')
time.sleep(2)
print('\nResultat COUNTINGWORDS amb 1 fitxer:')
print(proxy.getResult(str(job_id), 'countingWords'))

#COUNTINGWORDS amb 1 fitxer
job_id = proxy.tractamentCua('countingWords', 'fitxerText2.txt')
time.sleep(2)
print('\nResultat COUNTINGWORDS amb 1 fitxer:')
print(proxy.getResult(str(job_id), 'countingWords'))

#COUNTINGWORDS amb 2 fitxers
job_id = proxy.tractamentCua('countingWords', 'fitxerText.txt*fitxerText2.txt')
time.sleep(5)
print('\nResultat COUNTINGWORDS amb 2 fitxers:')
print(proxy.getResult(str(job_id), 'countingWords'))

#WORDCOUNT amb 3 fitxers
job_id = proxy.tractamentCua('wordCount', 'fitxerText.txt*fitxerText2.txt*fitxerText.txt')
time.sleep(5)
print('\nResultat WORDCOUNT amb 3 fitxers:')
print(proxy.getResult(str(job_id), 'wordCount'))

