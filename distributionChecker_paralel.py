import multiprocessing as mp
import random
import string
import os
from time import time


data_address = os.path.join(os.getcwd(), "FimmX_accel1S_S13_R2_001.fastq")
duplicatesfile_address = os.path.join(os.getcwd(), "Duplicates.csv")

#data_address = os.path.join(os.getcwd(), "WORKING", "projects", "DNAm_testis", "accel_adaptase", "Temp_accel1s", "FimmX_accel1S_S13_R2_001.fastq")
#duplicatesfile_address = os.path.join(os.getcwd(),"WORKING", "projects", "DNAm_testis", "accel_adaptase", "Temp_accel1s", "Duplicates.csv")


random.seed(123)

# Define an output queue
output = mp.Queue()

# define a example function
#def rand_string(length, output):
#    """ Generates a random string of numbers, lower- and uppercase chars. """
#    rand_str = ''.join(random.choice(
#                        string.ascii_lowercase
#                        + string.ascii_uppercase
#                        + string.digits)
#                   for i in range(length))
#    output.put(rand_str)

# Setup a list of processes that we want to run
#processes = [mp.Process(target=rand_string, args=(5, output)) for x in range(4)]

# Run processes
#for p in processes:
#    p.start()

# Exit the completed processes
#for p in processes:
#    p.join()

# Get process results from the output queue
#results = [output.get() for p in processes]

#print(results)



ERROR_MARGIN = 3

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
def getDictionary(fileName):
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
                start = line[:8]
                if("N" in start):
                    continue
                end = line[8:].strip()
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


'''O(N*M) where N is the number of entries in the file and M is average number of entries per start'''
def generateDuplicatesFile(openings, repeats):
    for key in openings.keys():
        duplicated = countDuplicatesFor(key, openings)
        #print(key,"was repeated", len(openings[key]),"times, of which there were", duplicated,"duplicates.")
        repeats[key] = (len(openings[key]), duplicated)
    

#    with open(outputName, "w") as outFile:
#        outFile.write("START, X, Y"+os.linesep)
#        for key in repeats.keys():
#            outFile.write(key+", "+str(repeats[key][0])+", "+str(repeats[key][1])+os.linesep)    

sTime = time()

openings = getDictionary(data_address)

results = []
opening_keys = list(openings.keys())
devision = 4
processes = []

for i in range(0, devision):
    subdictionary = {}
    for j in range(int(i*len(opening_keys)/devision), int((i+1)*len(opening_keys)/devision)):
        subdictionary[opening_keys[j]]=openings[opening_keys[j]]

    output = {}
    currentprocess = mp.Process(target=generateDuplicatesFile, args=(subdictionary, output))
    processes.append(currentprocess)
    results.append(output)
    currentprocess.start()

for p in processes:
    p.join()

joint_dick = {}
for i in range(0, len(results)):
    for key in results[i].keys():
        joint_dick[key]=results[i][key]
print(joint_dick)


#split_openings = []
#opening_keys = list(openings.keys())
#first = {} 
#for i in range(0, int(len(opening_keys)/2)):
#    first[opening_keys[i]]=openings[opening_keys[i]]
#second = {} 
#for i in range(int(len(opening_keys)/2), len(opening_keys)):
#    second[opening_keys[i]]=openings[opening_keys[i]]
#split_openings.append(first)
#split_openings.append(second)

#output1 = {}
#output2 = {}

#processes = [mp.Process(target=generateDuplicatesFile, args=(first, output1)),
#             mp.Process(target=generateDuplicatesFile, args=(second, output2))]

#for p in processes:
#    p.start()

# Exit the completed processes
#for p in processes:
#    p.join()

#joint_dick = {}
#for key in output1.keys():
#    joint_dick[key]=output1[key]
#for key in output2.keys():
#    joint_dick[key]=output2[key]
#print(joint_dick)

eTime = time()

print("Parallel took", eTime-sTime, "seconds")

print("FINISHED")


#stripStart(data_address), "Stripped_Marcin.txt")

#sTime = time()
#openings = getDictionary(data_address)
#generateDuplicatesFile(openings, {}) #This will be slow

'''
#Report all starts with duplicates
duplicates = []
with open(duplicatesfile_address, 'r') as myFile:
    header = True
    for line in myFile:
        if(header):
            header=False
            continue
        segments = line.split(",")
        if(int(segments[2].strip())>0):
            #print(segments[0], "is repeated", segments[1],"times, of which", segments[2].strip(),"are duplicates.")
            duplicates.append(segments[0])

for i in range(0, len(duplicates)):
    print("Reporting duplicates for", duplicates[i],"-")
    reportDuplicatesFor(duplicates[i], openings)
'''

#total = 0
#for key in openings.keys():
#    total += len(openings[key])
#print("Out of", total, "entries a total of", len(openings.keys()),"unique openings were found out of a possible", 4**8)



#eTime = time()

#print("Single took", eTime-sTime, "seconds")


#print("FINISHED")

#reportDuplicatesFor("GGGGGATT", openings)

