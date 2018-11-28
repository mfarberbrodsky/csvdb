from __future__ import print_function
import sqltokenizer


class CSVDBSyntaxError(ValueError):
    def __init__(self, message, line, col, text):
        super().__init__()
        self.line = line
        self.col = col
        self.text = text
        self.message = "CSVDB Syntax error at line {}  col {}: {}".format(line, col, message)

    def show_error_location(self):
        """Returns a string with the original string and the location of the syntax error"""
        s = ""
        for i, line_text in enumerate(self.text.splitlines() + ["\n"]):
            s += line_text
            if i == self.line:
                s += "=" * (self.col - 1) + "^^^\n"
        return s

    def __str__(self):
        return self.message


class BaseSyntaxNode(object):
    """base class for all syntax-tree nodes"""
    pass

class NodeCreate(BaseSyntaxNode):
    pass

class NodeDrop(BaseSyntaxNode):
    def __init__(self, table_name, allow_not_exists):
        self.table_name = table_name  # table to drop
        self.allow_not_exists =  allow_not_exists

class NodeLoad(BaseSyntaxNode):
    def __init__(self, infile_name, table_name, ignore_lines):
        self.infile_name = infile_name
        self.table_name = table_name
        self.ignore_lines = ignore_lines

class NodeSelect(BaseSyntaxNode):
    pass


class SqlParser(object):
    def __init__(self, text):
        self._text = text
        self._tokenizer = sqltokenizer.SqlTokenizer(text)
        self._line = None
        self._col = None
        self._token = None
        self._val = None  # current token value

    def _next_token(self):
        self._line, self._col = self._tokenizer.cur_text_location()
        self._token, self._val = self._tokenizer.next_token()

    def _expect_next_token(self, token, expected_val_or_none=None):
        self._next_token()
        self._expect_cur_token(token, expected_val_or_none)

    def _expect_cur_token(self, token, expected_val_or_none=None):
        if self._token != token:
            self._raise_error("Unexpected token: {}:{} (expecting {})".format(self._token, self._val, token) )
        if expected_val_or_none is not None and self._val != expected_val_or_none:
            self._raise_error("Unexpected token value: " + str(self._val))

    def parse_single_command(self):
        """Parse a single command and return syntax-tree-node.
        If no command (EOF) return None."""
        self._next_token()
        tok = self._token
        val = self._val
        if tok == sqltokenizer.SqlTokenKind.EOF:
            return None
        self._expect_cur_token(sqltokenizer.SqlTokenKind.KEYWORD)
        if val == "create":
            return self._parse_create()
        elif val == "drop":
            return self._parse_drop()
        elif val == "load":
            return self._parse_load()
        elif val == "select":
            return self._parse_select()
        else:
            self._raise_error("Unexpected commad: " + str(self._val))

    def parse_multi_commands(self):
        """Parse SQL commands, retrn a list of the Syntax Tree-root-node for each command"""
        nodes = []
        while True:
            node = self.parse_show_error()
            if not node:
                return nodes
            nodes.append(node)


    def _raise_error(self, message):
        raise CSVDBSyntaxError(message, self._line, self._col, self._text)

    def _parse_drop(self):
        # syntax: DROP TABLE identifier
        self._expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, "table")
        self._next_token()
        allow_not_exists = False
        if self._token == sqltokenizer.SqlTokenKind.KEYWORD and self._val == "if":
            self._expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, "exists")
            self._next_token()
            allow_not_exists = True

        self._expect_cur_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
        table_name = self._val
        self._expect_next_token(sqltokenizer.SqlTokenKind.OPERATOR, ";")
        return NodeDrop(table_name, allow_not_exists)

    def _parse_load(self):
        # syntax:
        # LOAD DATA INFILE string_literal
        # INTO TABLE identifier
        # [IGNORE literal_number LINES];

        self._expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, "data")
        self._expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, "infile")
        self._expect_next_token(sqltokenizer.SqlTokenKind.LIT_STR)
        infile_name = self._val
        self._expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, "into")
        self._expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, "table")
        self._expect_next_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
        table_name = self._val
        ignore_lines = 0
        self._next_token()
        if self._token == sqltokenizer.SqlTokenKind.OPERATOR and self._val == ";":
            # got to end of command
            return NodeLoad(infile_name, table_name, ignore_lines)
        # not end of command - optional IGNORE clause
        self._expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, "ignore")
        self._expect_next_token(sqltokenizer.SqlTokenKind.LIT_NUM)
        ignore_lines = self._val
        self._expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, "lines")
        self._expect_next_token(sqltokenizer.SqlTokenKind.OPERATOR, ";")
        return NodeLoad(infile_name, table_name, ignore_lines)

    def _parse_create(self):
        raise NotImplemented("TODO - CREATE command Put your code here")

    def _parse_select(self):
        raise NotImplemented("TODO - SELECT command Put your code here")

    def parse_show_error(self):
        try:
            return self.parse_single_command()
        except CSVDBSyntaxError as ex:
            print(ex.show_error_location())
            raise ex


def _test():
    text = """
    drop  movies;"""
    parser = SqlParser(text)
    parser.parse_show_error()

if __name__ == "__main__":
    _test()

