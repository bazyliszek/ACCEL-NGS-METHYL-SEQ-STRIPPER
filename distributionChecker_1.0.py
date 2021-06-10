'''
Created on 15 Sep 2018

'''
import os
from time import time


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
def generateDuplicatesFile(openings, outputName):
    repeats = {}
    for key in openings.keys():
        duplicated = countDuplicatesFor(key, openings)
        #print(key,"was repeated", len(openings[key]),"times, of which there were", duplicated,"duplicates.")
        repeats[key] = (len(openings[key]), duplicated)
    

    with open(outputName, "w") as outFile:
        outFile.write("START, X, Y"+os.linesep)
        for key in repeats.keys():
            outFile.write(key+", "+str(repeats[key][0])+", "+str(repeats[key][1])+os.linesep)    
    
    


#stripStart(os.path.join(os.getcwd(),"FimmX_accel1S_S13_R2_001.fastq"), "Stripped_8bases.txt")

sTime = time()
openings = getDictionary(os.path.join(os.getcwd(),"FimmX_accel1S_S13_R2_001.fastq"))

generateDuplicatesFile(openings, "TestOut.txt") #This will be slow


#Report all starts with duplicates
duplicates = []
with open("TestOut.txt", 'r') as myFile:
    header = True
    for line in myFile:
        if(header):
            header=False
            continue
        segments = line.split(",")
        if(int(segments[2].strip())>0):
            #print(segments[0], "is repeated", segments[1],"times, of which", segments[2].strip(),"are duplicates.")
            duplicates.append(segments[0])

#for i in range(0, len(duplicates)):
for i in range(0, 10):
    print("Reporting duplicates for", duplicates[i],"-")
    reportDuplicatesFor(duplicates[i], openings)


#total = 0
#for key in openings.keys():
#    total += len(openings[key])
#print("Out of", total, "entries a total of", len(openings.keys()),"unique openings were found out of a possible", 4**8)



eTime = time()

print("Took", eTime-sTime, "seconds")


print("FINISHED")

#reportDuplicatesFor("GGGGGATT", openings)

