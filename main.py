import sys
import repo as rep


def main(args):
    configPath = args[1]
    ordersPath = args[2]
    outputPath = args[3]
    repo = rep.repo
    repo.create_tables()
    config_parser(configPath, repo)  # parse the config file
    orders_parser(ordersPath, outputPath, repo)


if __name__ == '__main__':
    main(sys.argv)


def config_parser(inputFile, repo):
    with open(inputFile, encoding="utf-8") as inputfile:
        for i, line in inputfile:
            line_array = line.split(',')
            if i == 0:
                end_of_vac = line_array[0]  # 3 : indexes 1 2 3
                end_of_sup = end_of_vac + line_array[1]  # 3+1 : indexes 4
                end_of_clin = end_of_sup + line_array[2]  # 4+2 : indexes 5 6
            if 0 < i <= end_of_vac:  # vaccines
                repo.vaccines.insert(*line_array)
            if end_of_vac < i <= end_of_sup:  # suppliar
                repo.suppliers.insert(*line_array)
            if end_of_sup < i <= end_of_clin:  # clincs
                repo.clinics.insert(*line_array)
            if i > end_of_clin:  # logistics
                repo.logistics.insert(*line_array)
    inputfile.close()


def orders_parser(inputfile, outputpath, repo):
    with open(inputfile, encoding="utf-8") as inputfile:
        for line in inputfile:
            line_array = line.split(',')
            if len(line_array) == 2:
                repo.send_shipment(*line_array)
            if len(line_array) == 3:
                repo.receive_shipment(*line_array)
            log_array = repo.action_log()
            with open(outputpath, "w") as outputFile:
                outputFile.write(','.join(log_array))
                outputFile.close()
    inputfile.close()
