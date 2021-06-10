import multiprocessing as mp
import os
from time import time, sleep


TAIL_LENGTH = 0
ERROR_MARGIN = 3
PROCESSES = 20

fastq_address = os.path.join(os.getcwd(), "FimmX_accel1S_S13_R2_001.fastq")
duplicates_address = os.path.join(os.getcwd(), "Duplicates_15102018_CG.csv")


def stripStart(fileName, outputName):
    with open(fileName, 'r') as inFile:
        with open(outputName, 'w') as outFile:
            i = 0
            for line in inFile:
                i+=1
                if(i%4 == 2):
                    start = line[:8]
                    if("N" in start):
                        continue
                    print(start)
                    outFile.write(start+os.linesep)

'''O(N)'''
def getDictionary(fileName, tail):
    '''{8LetterStart : [List of all endings]}'''
    openingCombinations = {}
    with open(fileName, 'r') as myFile:
        i = 0
        for line in myFile:
            i+=1
            if(i%4==1):
                segments = line.split(":")
                location = (segments[5], segments[6].split(" ")[0])
            if(i%4 == 2):
                start = line[:tail]
                if("N" in start):
                    continue
                end = line[tail:].strip()
                if(start in openingCombinations.keys()):
                    openingCombinations[start].append((end, location))
                else:
                    openingCombinations[start] = [(end, location)]
    return openingCombinations


'''O(N)'''
def getVariableDictionary(fileName):
    '''{Tail : [List of all endings]}
    Tail includes the first letter then any number of Gs and As'''
    openingCombinations = {}
    with open(fileName, 'r') as myFile:
        i = 0
        for line in myFile:
            i+=1
            if(i%4==1):
                segments = line.split(":")
                location = (segments[5], segments[6].split(" ")[0])
            if(i%4 == 2):
                n_found = False
                for j in range(1, len(line)):
                    letter = line[j]
                    if(letter == "N"):
                        n_found = True
                        break
                    elif(letter == "T" or letter == "C"):
                        break
                if(n_found or line[0] == "N"):
                    continue
                
                start = line[:j].strip()
                end = line[j:].strip()
                if(start in openingCombinations.keys()):
                    openingCombinations[start].append((end, location))
                else:
                    openingCombinations[start] = [(end, location)]
    return openingCombinations


def stringCompare(str1, str2, errorMargin):
    errors = 0
    for i in range(0, len(str1)):
        if(str1[i] != str2[i]):
            if(str1[i]!= "N" and str2[i]!= "N"):
                errors += 1
                if(errors > errorMargin):
                    return errors
    return errors

def headDictionary(diction, headLength):
    i = 0
    for key in diction.keys():
        print(key, ":", diction[key])
        i+= 1
        if i >= headLength:
            break

'''O(A)'''
def mostRepeated(openingCombinations):
    best = ""
    repeats = 0
    for key in openingCombinations.keys():
        length = len(openingCombinations[key])
        if(length > repeats):
            repeats = length
            best = key
    return best
            

'''O(N)'''
def countDuplicatesFor(startString, openingCombinations):
    #print(startString, "was repeated", len(openingCombinations[startString]), "times")
    knownList = []
    duplicates = 0
    for i in range(0, len(openingCombinations[startString])):
        end = openingCombinations[startString][i][0]
        
        foundAt = -1
        for j in range(0,len(knownList)):
            errors = stringCompare(knownList[j], end, ERROR_MARGIN)
            if(errors <= ERROR_MARGIN):
                foundAt = j
                break
        if(foundAt > -1):
            #print(end, "Already found at index", foundAt)
            duplicates+=1
        else:
            knownList.append(end)
    #print("Found", duplicates, "duplicates")
    return duplicates

'''O(B^2)'''
def reportDuplicatesFor(startString, openingCombinations):
    ends = openingCombinations[startString]
    knownList = []
    for i in range(0, len(ends)):
        end = ends[i][0]
        foundAt = -1
        for j in range(0,len(knownList)):
            errors = stringCompare(knownList[j], end, ERROR_MARGIN)
            if(errors <= ERROR_MARGIN):
                foundAt = j
                break
        if(foundAt > -1):
            print(knownList[foundAt], "differs by",errors,"from")
            print(end)
            print("")
        else:
            knownList.append(end)

def loadDuplicatesFile(address):
    duplicates = {}
    header = True
    with open(address, 'r') as myFile:
        for line in myFile:
            if(header):
                header = False
            else:
                separated = line.split(",")
                duplicates[separated[0].strip()] = (int(separated[1].strip()), separated[2].strip())

def writeDuplicatesFilePar(openings, duplicates_address, divisions):
    opening_keys = list(openings.keys())
    results = mp.Queue()
    processes = []
    
    for i in range(0, divisions):
        subdictionary = {}
        if(i+1 == divisions):#prevents rounding errors from cutting the last element off the list
            for j in range(int(i*len(opening_keys)/divisions), len(opening_keys)):
                subdictionary[opening_keys[j]]=openings[opening_keys[j]]
        else:
            for j in range(int(i*len(opening_keys)/divisions), int((i+1)*len(opening_keys)/divisions)):
                subdictionary[opening_keys[j]]=openings[opening_keys[j]]
    
        currentprocess = mp.Process(target=generateDuplicatesFilePar, args=(subdictionary, results))
        processes.append(currentprocess)
        currentprocess.start()
        
    
    #take the processed data out of the queue
    joint_dictionary = {}
    while any(p.is_alive() for p in processes):
        while not results.empty():
            subdict = results.get()
            for key in subdict.keys():
                joint_dictionary[key]=subdict[key]
                
    #Wait for all processes to finish
    for i in range(0, len(processes)):
        processes[i].join()
        print(i, "finished at", time()-sTime)
    
    
    with open(duplicates_address, "w") as outFile:
        outFile.write("START, X, Y"+os.linesep)
        for key in joint_dictionary.keys():
            outFile.write(key+", "+str(joint_dictionary[key][0])+", "+str(joint_dictionary[key][1])+os.linesep)  
    
    return joint_dictionary

'''O(N*M) where N is the number of entries in the file and M is average number of entries per start'''
def generateDuplicatesFilePar(openings, queue):
    repeats={}
    for key in openings.keys():
        duplicated = countDuplicatesFor(key, openings)
        #print(key,"was repeated", len(openings[key]),"times, of which there were", duplicated,"duplicates.")
        repeats[key] = (len(openings[key]), duplicated)
    print("process", os.getpid(), ": adding to queue")
    queue.put(repeats)
    print(1)
    queue.close()
    print(2)
    queue.join_thread()
    print("process", os.getpid(), ": FINISHED adding to queue")


def writeDuplicatesFile(openings, duplicates_address):
    
    duplicates = generateDuplicatesFile(openings)
    
    with open(duplicates_address, "w") as outFile:
        outFile.write("START, X, Y"+os.linesep)
        for key in duplicates.keys():
            outFile.write(key+", "+str(duplicates[key][0])+", "+str(duplicates[key][1])+os.linesep)  
    return duplicates

def generateDuplicatesFile(openings):
    repeats={}
    for key in openings.keys():
        duplicated = countDuplicatesFor(key, openings)
        #print(key,"was repeated", len(openings[key]),"times, of which there were", duplicated,"duplicates.")
        repeats[key] = (len(openings[key]), duplicated)
    return repeats
    

#    with open(outputName, "w") as outFile:
#        outFile.write("START, X, Y"+os.linesep)
#        for key in repeats.keys():
#            outFile.write(key+", "+str(repeats[key][0])+", "+str(repeats[key][1])+os.linesep)    


openings = getVariableDictionary(fastq_address)

if(fastq_address != None):
    if(TAIL_LENGTH == 0):
        openings = getVariableDictionary(fastq_address)
    else:
        openings = getDictionary(fastq_address, TAIL_LENGTH)



sTime = time()

print(sTime)

if(PROCESSES > 1):
    if(fastq_address != None and duplicates_address != None):
        duplicates = writeDuplicatesFilePar(openings, duplicates_address, PROCESSES)
    elif(duplicates_address != None):
        duplicates = loadDuplicatesFile(duplicates_address)
    eTime = time()
    
    print("Parallel took", eTime-sTime, "seconds")

else:
    pass
    #stripStart(fastq_address), "Stripped_Marcin.txt")
    if(fastq_address != None and duplicates_address != None):
        duplicates = writeDuplicatesFile(openings, duplicates_address) #This will be slow
    elif(duplicates_address != None):
        duplicates = loadDuplicatesFile(duplicates_address)
        
    eTime = time()
    
    print("Single took", eTime-sTime, "seconds")


print("FINISHED")

#reportDuplicatesFor("GGGGGATT", openings)

