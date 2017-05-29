'''
	A pre processing script to read every tweet from the given file
	and output the mentions and hashtags into a processed tweets file.

	Input - inputFile and outputfile (along with extensions) - Command Line
	Output - Mentions and Hashtags in the outputFile
'''
import sys
import string

inputFile = str(sys.argv[1])
outputfile = str(sys.argv[2])

file = open(inputFile).readlines()
output = open(outputFile, 'w')

for line in file:
	if line != "\n":
		split = line.split(" ")
		for word in split:
			if len(word) > 2:
				if word[0] == "@" or word[0] == "#":
					x = word[1:]
					y = ""
					for i in word[1:]:
						if i.isalpha():
							y+=i.lower()
					y = word[0] + y
					output.write(y + "\n")