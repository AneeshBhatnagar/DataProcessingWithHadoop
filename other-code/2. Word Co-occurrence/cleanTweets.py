'''
	Script to clean tweets for the 2nd activity.
'''
import sys

inputFile = str(sys.argv[1])
outputFile = str(sys.argv[2])

f = open(inputFile).readlines()
output = open(outputFile,'w')
for line in f:
	op = ''.join(char.lower() for char in line if (char.isalpha() or char.isspace()))
	output.write(op+"\n")