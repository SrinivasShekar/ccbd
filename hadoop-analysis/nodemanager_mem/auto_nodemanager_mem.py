#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
import csv
import xml.etree.ElementTree as ET
import subprocess
coreName = []
coreValue = []
hdfsName = []
hdfsValue = []
mapredName = []
mapredValue = []
yarnValue = []
yarnName = []


def yarnSiteChange():
    tree = ET.parse('/usr/local/hadoop/etc/hadoop/yarn-site.xml')
    root = tree.getroot()

    

    propertyTag5 = ET.SubElement(root, 'property')
    nameTag5 = ET.SubElement(propertyTag5, 'name')
    valueTag5 = ET.SubElement(propertyTag5, 'value')
    nameTag5.text = 'yarn.nodemanager.resource.memory-mb'

    

    yarnValue.append([256,4096])
    
    newDir = '/usr/local/hadoop/logsYarn/nodemanager_mem'

    # subprocess.call(["mkdir",newDir])

    for i in [256,4096]:
        valueTag5.text = str(i)
        tree.write('/usr/local/hadoop/etc/hadoop/yarn-site.xml')
        subprocess.call(['start-all.sh'])
        while True:
            a = subprocess.check_output(['hadoop','dfsadmin', '-safemode', 'get'])
            if a[13:15].decode('utf-8') == 'OF':
                   print('breaking out of first safe mode checks\n')
                   break
                   time.sleep(5)

        fileNameOutput = newDir + '/combo-' + str(i) +  '-Output'
        nnMonitor = newDir + '/combo-' + str(i) + '-nnMonitor'
        dnMonitor = newDir + '/combo-' + str(i) + '-dnMonitor'
        
        rmMonitor = newDir + '/combo-' + str(i) + '-rmMonitor'
        fileNameStdout = newDir + '/combo-' + str(i)  + '-stdout'
        fileNameStderr = newDir + '/combo-' + str(i) + '-stderr'
        out_file = open(fileNameStdout, 'w')
        error_file = open(fileNameStderr, 'w')
        print('Running :' + ':' + str(i) + '\n')
        #try:
        subprocess.call(['sh','final.sh',fileNameOutput,nnMonitor,dnMonitor, rmMonitor], stdout=out_file,stderr=error_file, timeout=800)
            
        '''except:
             print('Timed out passing exeption')
             pass
        while True:
             a = subprocess.check_output(['hadoop','dfsadmin', '-safemode', 'get'])
             if a[13:15].decode('utf-8') == 'OF':
                print('Breaking out of safe mode checks\n')
                break
             time.sleep(5)

        subprocess.call(['stop-all.sh'])
        '''        
yarnSiteChange()

