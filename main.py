import atexit
import sys
import repo as rep


def main(args):
    configPath = args[1]
    ordersPath = args[2]
    outputPath = args[3]
    repo = rep._Repository()
    repo.create_tables()
    config_parser(configPath, repo) #parse the config file
    orders_parser(ordersPath, outputPath, repo)
    atexit.register(repo._close)  # TODO: figure out what is this line


if __name__ == '__main__':
    main(sys.argv)


def config_parser(inputFile, repo):
    with open(inputFile, encoding="utf-8") as inputfile:
        for i, line in inputfile:
            lineArray = line.split(',')
            if i==0:
                endOfVac = lineArray[0] # 3 : indexes 1 2 3
                endOfSup = endOfVac+lineArray[1] # 3+1 : indexes 4
                endOfClin = endOfSup +lineArray[2] # 4+2 : indexes 5 6
            if 0 < i <= endOfVac: #vaccines
                repo.vaccines.insert(*lineArray)
            if endOfVac < i <= endOfSup: #suppliar
                repo.suppliers.insert(*lineArray)
            if endOfSup < i <= endOfClin: #clincs
                repo.clinics.insert(*lineArray)
            if i > endOfClin: #logistics
                repo.logistics.insert(*lineArray)
    inputfile.close()


def orders_parser(inputfile, outputpath, repo):
    with open(inputfile, encoding="utf-8") as inputfile:
        for line in inputfile:
            lineArray = line.split(',')
            if len(lineArray) ==2:
                repo.send_shipment(*lineArray)
            if len(lineArray) ==3:
                repo.receive_shipment(*lineArray)
            logArray = repo.action_log()
            with open(outputpath, "w") as outputFile:
                outputFile.write(','.join(logArray))
                outputFile.close()
    inputfile.close()


