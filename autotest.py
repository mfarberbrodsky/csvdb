import os
import Parser
import CSVDBErrors

test_dir = "./csvdb_tests"

tests = os.listdir(test_dir)
num_total = len(tests)
num_passed = 0

for test in tests:
    print("Running test " + test + "...")
    curr_test_dir = os.path.join(test_dir, test)

    is_exception = False
    with open(os.path.join(curr_test_dir, 'test.sql'), 'r') as f:
        command_text = f.read()
        command_parser = Parser.Parser(command_text)
        while True:
            try:
                command_node = command_parser.parse_command()
            except CSVDBErrors.CSVDBSyntaxError as e:
                print("Syntax error in test " + test + ": " + str(e))
                is_exception = True
                break
            if command_node is None:
                break

            try:
                command_node.execute(curr_test_dir)
            except Exception as e:
                print("Runtime error in test " + test + ": " + str(e))
                is_exception = True
                break

    if not is_exception:
        with open(os.path.join(curr_test_dir, "good_output.csv")) as f:
            expected_text = f.read()
        with open(os.path.join(curr_test_dir, "output.csv")) as f:
            actual_text = f.read()
        if "\n".join([s for s in expected_text.split("\n") if s]) == "\n".join(
                [s for s in actual_text.split("\n") if s]):
            print("Passed test " + test + "\n")
            num_passed += 1
        else:
            print("Failed test " + test + "\n")
    else:
        print("Failed test " + test + "\n")

    to_remove = ["output.csv", "test/table.json", "test/test.csv"]
    for file in to_remove:
        if os.path.exists(os.path.join(curr_test_dir, file)):
            os.remove(os.path.join(curr_test_dir, file))
    if os.path.exists(os.path.join(curr_test_dir, "test")):
        os.rmdir(os.path.join(curr_test_dir, "test"))

print("Passed " + str(num_passed) + " out of " + str(num_total) + " tests")
