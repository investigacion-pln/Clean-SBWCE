#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
####################################################

Detección y Extracción de Líneas de Texto del Corpus 
SBWCE que se encuentran en Idiomas Diferentes al 
español


Created 26-06-2018 15:07:22 

@author: Orlando Montenegro
         orlando.montenegro@correounivalle.edu.co

####################################################
"""
from __future__ import print_function
from langdetect import detect
import time
import os
from joblib import Parallel, delayed

os.system("taskset -p 0xff %d" % os.getpid())

Lang = {}

# se inicializan las variables comunes, que representan las rutas donde se 
# encuentra el corpus, el nombre base de los archivos y la ruta donde se 
# almacenaran los nuevos archivos procesados
basepath      = 'corpus/spanish_billion_words/'
basepath_cl   = 'corpus/clean/'
base_filename = 'spanish_billion_words_'
file_results  = 'result.txt'

# esta funcion obtiene el numero de lineaas de un archivo de texto
def getNumLines(filename):
    return str(sum(1 for line in open(filename)))

# esta funcion detecta el idioma de una cadena de entrada
def getIdiom(sentence):
    try:
        idiom = detect(sentence)
    except:
        # si no se detecta el idioma se envia un tag nlg 'no lenguaje' en 
        # la captura de la excepcion
        idiom = 'nlg'
    idiom = detect(sentence)
    return str(idiom)

# Funcion que desarrolla la logica de extracion de lineas de los archivos
# tiene como entrada un entero x, que representa el archivo que se esta procesando
def getData(x):
    # Se inicia el archivo de resultados recopilando la informacion basica de cada 
    # archivo procesado del corpus
    file_rest = open(file_results, "a")
    archiev = base_filename + str(x).zfill(2)
    filename = basepath + archiev
    print ('\nArchivo ' + archiev)
    file_rest.write('\nArchivo ' + archiev + os.linesep)
    print ('Numero de Lineas: ' + getNumLines(filename))
    file_rest.write('Numero de Lineas: ' + getNumLines(filename) + os.linesep)
    
    # se abre el archivo que no contendra las lineas de texto en otros idiomas
    file_clean = open(basepath_cl + archiev + '_clean', "w")
    
    with open(filename) as file:
        # se recorree el archivo linea por linea
        for l in file:
            # se decodifica cada linea para poder desarrollar la deteccion de lenguaje
            sentences = l.strip().decode('utf-8')
            
            # se invoca la funcion de deteccion de idioma
            lang = getIdiom(sentences)
             
            # si el idioma detectado esta en español, se procede a almacenarlo 
            # en el nuevo archivo en caso contrario se descarta
            if lang == 'es':
                file_clean.write(sentences.encode('utf-8') + os.linesep)
            
            # Se desarrolla el conteo de idiomas presentes en el archivo del
            # corpus que se esta procesando
            if lang not in Lang:
                Lang[lang] =  1
            else:
                Lang[lang] =  Lang[lang] + 1
                
    # se cierra el arrhivo con los datos ya procesados
    file_clean.close()
    
    # se escribe los resultados de la cantidad de idiomas detectados en el archivo
    # procesado
    for i in Lang:
        file_rest.write(str(i) + ' ' + str(Lang[i]) + os.linesep)
    
    # se limpia el conjunto de idiomas detectados
    Lang.clear()
    
    # se procede a escribir el tiempo final de procesamiento de cada archivo
    print ('Tiempo de Procesamiento', time.clock() - start_time, " segundos")
    file_rest.write('Tiempo de Procesamiento ' + str(time.clock() - start_time) + " segundos" + os.linesep)
    file_rest.close()

# funcion principal
if __name__ == "__main__":
    # se inicia la variable tiempo de inicio
    start_time = time.clock()
    
    # se usa la libreria Joblib y su metodo parallel con el cual se usan todos los
    # procesadores de la CPU
    pool = Parallel(n_jobs=-1, verbose=50, pre_dispatch='all')(delayed(
            getData)(x) for x in range(0, 100))
    
    # Se imprime el tiempo total de jecucion del procesamiento de todos los archivos
    # del corpus
    print ('\nTiempo Total de Procesamiento', time.clock() - start_time, " segundos")
