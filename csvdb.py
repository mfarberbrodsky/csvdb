import argparse
import logging
import Parser
import CSVDBErrors

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger()

parser = argparse.ArgumentParser(prog='csvdb.py', description='csvdb project by Yuval Cohen and Maya Farber Brodsky')
parser.add_argument('--rootdir', dest='path', default='.', help='directory under which all table directories reside')
parser.add_argument('--run', dest='filename', help='execute all SQL commands inside FILENAME')
parser.add_argument('--verbose', '-v', action='store_true', help='print log messages for debugging')
args = parser.parse_args()

if args.verbose:
    logger.setLevel(logging.DEBUG)  # Debug, Info, Warning, Error, Critical
else:
    logger.setLevel(logging.CRITICAL)

rootdir = args.path

if args.filename is None:
    print("csvdb>", end=" ")
    command = input()
    while command != "exit":
        command_node = None
        try:
            command_parser = Parser.Parser(command)
            command_node = command_parser.parse_command()
        except CSVDBErrors.CSVDBSyntaxError as e:
            logger.error(e)

        if command_node is not None:
            try:
                command_node.execute(rootdir)
            except ValueError as e:
                logger.error(e)

        print("csvdb>", end=" ")
        command = input()
else:
    pass
