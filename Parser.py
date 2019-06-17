import sqltokenizer
import NodeCommands
import CSVDBErrors


class Parser:
    def __init__(self, text):
        self.text = text
        self.tokenizer = sqltokenizer.SqlTokenizer(text)
        self.line = None
        self.col = None
        self.token = None
        self.val = None

    def next_token(self):
        self.line, self.col = self.tokenizer.cur_text_location()
        self.token, self.val = self.tokenizer.next_token()

    def expect_next_token(self, token, val=None):  # Raises CSVDBSyntaxError
        self.next_token()
        self.expect_cur_token(token, val)

    def expect_cur_token(self, token, val=None):  # Raises CSVDBSyntaxError
        if self.token != token:
            self.raise_error("Unexpected token: {}:{} (expecting {})".format(self.token, self.val, token))
        if val is not None and self.val != val:
            self.raise_error("Unexpected token value: " + str(self.val))

    def parse_condition(self, is_having):
        # Field
        if not is_having:
            self.expect_cur_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
            field_name = self.val
        else:
            if self.token == sqltokenizer.SqlTokenKind.IDENTIFIER:  # No aggregator
                field_name = self.val
            else:  # Aggregator
                self.expect_cur_token(sqltokenizer.SqlTokenKind.KEYWORD)
                agg = self.val
                self.expect_next_token(sqltokenizer.SqlTokenKind.OPERATOR, "(")
                self.expect_next_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
                field_name = (agg, self.val)
                self.expect_next_token(sqltokenizer.SqlTokenKind.OPERATOR, ")")
        self.next_token()

        # Operator
        if self.token == sqltokenizer.SqlTokenKind.IDENTIFIER:  # is / is not
            self.expect_cur_token(sqltokenizer.SqlTokenKind.IDENTIFIER, 'is')
            op = "is"
            self.next_token()
            if self.token == sqltokenizer.SqlTokenKind.KEYWORD and self.val == 'not':
                op = "is not"
                self.next_token()
        else:  # other operator
            self.expect_cur_token(sqltokenizer.SqlTokenKind.OPERATOR)
            op = self.val
            self.next_token()

        # Value
        if self.token == sqltokenizer.SqlTokenKind.KEYWORD:  # null
            self.expect_cur_token(sqltokenizer.SqlTokenKind.KEYWORD, 'null')
            value = 'null'
        elif self.token == sqltokenizer.SqlTokenKind.LIT_STR:  # string
            value = self.val
        else:  # number
            self.expect_cur_token(sqltokenizer.SqlTokenKind.LIT_NUM)
            value = self.val

        self.next_token()

        return field_name, op, value

    def parse_create(self):
        # Syntax:
        # CREATE TABLE [IF NOT EXISTS] _table_name_ (
        #       [ _name_  _type_ ,]*
        #       _name_  _type_
        # );
        #
        # CREATE TABLE _table_name_ AS _select_command_;

        if_not_exists = False
        table_name = ""
        fields = []
        as_select = None

        # Create table
        self.expect_cur_token(sqltokenizer.SqlTokenKind.KEYWORD, "create")
        self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, "table")
        self.next_token()

        # If not exists
        if self.token == sqltokenizer.SqlTokenKind.KEYWORD and self.val == "if":
            self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, "not")
            self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, "exists")
            if_not_exists = True
            self.next_token()

        # Table name
        self.expect_cur_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
        table_name = self.val

        self.next_token()

        if self.token == sqltokenizer.SqlTokenKind.KEYWORD and self.val == 'as':  # As select
            if if_not_exists:
                self.raise_error("IF NOT EXISTS is not supported in CREATE AS SELECT")
            self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, "select")
            as_select = self.parse_select()
        else:  # Field list
            self.expect_cur_token(sqltokenizer.SqlTokenKind.OPERATOR, '(')

            # First field
            self.expect_next_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
            field_name = self.val
            self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD)
            if self.val not in ['int', 'float', 'varchar', 'timestamp']:
                self.raise_error("type has to be int, float, varchar or timestamp")
            field_type = self.val
            fields.append({"field": field_name, "type": field_type})
            self.next_token()

            while not (self.token == sqltokenizer.SqlTokenKind.OPERATOR and self.val == ')'):
                self.expect_cur_token(sqltokenizer.SqlTokenKind.OPERATOR, ',')
                self.expect_next_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
                field_name = self.val
                self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD)
                if self.val not in ['int', 'float', 'varchar', 'timestamp']:
                    self.raise_error("type has to be int, float, varchar or timestamp")
                field_type = self.val
                fields.append({'field': field_name, 'type': field_type})
                self.next_token()
            self.expect_next_token(sqltokenizer.SqlTokenKind.OPERATOR, ';')

        return NodeCommands.NodeCreate(if_not_exists, table_name, fields, as_select)

    def parse_drop(self):
        # Syntax:
        # DROP TABLE [IF EXISTS] _table_name_;

        if_exists = False
        table_name = ""

        # Drop table
        self.expect_cur_token(sqltokenizer.SqlTokenKind.KEYWORD, 'drop')
        self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, 'table')
        self.next_token()

        # If exists
        if self.token == sqltokenizer.SqlTokenKind.KEYWORD and self.val == 'if':
            self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, 'exists')
            if_exists = True
            self.next_token()

        # Table name
        self.expect_cur_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
        table_name = self.val
        self.expect_next_token(sqltokenizer.SqlTokenKind.OPERATOR, ';')

        return NodeCommands.NodeDrop(if_exists, table_name)

    def parse_load(self):
        # Syntax:
        # LOAD DATA INFILE _file_name_string_
        # INTO TABLE _table_name_
        # [IGNORE _number_ LINES];

        infile_name = ""
        table_name = ""
        ignore_lines = 0

        # Load data infile
        self.expect_cur_token(sqltokenizer.SqlTokenKind.KEYWORD, 'load')
        self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, 'data')
        self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, 'infile')
        self.expect_next_token(sqltokenizer.SqlTokenKind.LIT_STR)
        infile_name = self.val

        # Into table
        self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, 'into')
        self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, 'table')
        self.expect_next_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
        table_name = self.val
        self.next_token()

        # Ignore ... lines
        if self.token == sqltokenizer.SqlTokenKind.KEYWORD and self.val == 'ignore':
            self.expect_next_token(sqltokenizer.SqlTokenKind.LIT_NUM)
            ignore_lines = self.val
            self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, 'lines')
            self.next_token()

        self.expect_cur_token(sqltokenizer.SqlTokenKind.OPERATOR, ';')

        return NodeCommands.NodeLoad(infile_name, table_name, ignore_lines)

    def parse_select(self):
        # Syntax:
        # SELECT [*|_expression_list_]
        # [INTO OUTFILE _file_name_string_]
        # FROM _table_name_
        # [WHERE _simple_condition_]
        # [GROUP BY _group_fields_]
        # [HAVING _group_condition_]
        # [ORDER BY _order_fields_]

        field_list = []
        outfile_name = None
        table_name = ""
        where = None
        group_by_list = []
        having = None
        order_by_list = []

        # Select
        self.expect_cur_token(sqltokenizer.SqlTokenKind.KEYWORD, 'select')
        self.next_token()

        # Fields
        if self.token == sqltokenizer.SqlTokenKind.OPERATOR and self.val == '*':
            field_list = ['*']
            self.next_token()
        else:
            more_fields = True
            while more_fields:
                agg = None
                if self.token == sqltokenizer.SqlTokenKind.IDENTIFIER:  # No aggregator
                    cur_name = self.val
                    self.next_token()
                else:  # Aggregator
                    self.expect_cur_token(sqltokenizer.SqlTokenKind.KEYWORD)
                    agg = self.val
                    self.expect_next_token(sqltokenizer.SqlTokenKind.OPERATOR, '(')
                    self.expect_next_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
                    cur_name = self.val
                    self.expect_next_token(sqltokenizer.SqlTokenKind.OPERATOR, ')')
                    self.next_token()

                # As
                output_name = None
                if self.token == sqltokenizer.SqlTokenKind.KEYWORD and self.val == 'as':
                    self.expect_next_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
                    output_name = self.val
                    self.next_token()

                field_list.append(NodeCommands.SelectField(cur_name, as_name=output_name, agg=agg))

                more_fields = (self.token == sqltokenizer.SqlTokenKind.OPERATOR and self.val == ',')
                if more_fields:
                    self.next_token()

        self.expect_cur_token(sqltokenizer.SqlTokenKind.KEYWORD)

        # Into outfile
        if self.token == sqltokenizer.SqlTokenKind.KEYWORD and self.val == 'into':
            self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, 'outfile')
            self.expect_next_token(sqltokenizer.SqlTokenKind.LIT_STR)
            outfile_name = self.val
            self.next_token()

        # From table
        self.expect_cur_token(sqltokenizer.SqlTokenKind.KEYWORD, 'from')
        self.expect_next_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
        table_name = self.val
        self.next_token()

        # Where
        if self.token == sqltokenizer.SqlTokenKind.KEYWORD and self.val == 'where':
            self.next_token()
            where = self.parse_condition(is_having=False)

        # Group by
        if self.token == sqltokenizer.SqlTokenKind.KEYWORD and self.val == 'group':
            self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, 'by')

            # Fields
            self.expect_next_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
            group_by_list.append(self.val)
            self.next_token()
            while self.token == sqltokenizer.SqlTokenKind.OPERATOR and self.val == ',':
                self.expect_next_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
                group_by_list.append(self.val)
                self.next_token()

            # Having
            if self.token == sqltokenizer.SqlTokenKind.KEYWORD and self.val == 'having':
                self.next_token()
                having = self.parse_condition(is_having=True)

        # Order by
        if self.token == sqltokenizer.SqlTokenKind.KEYWORD and self.val == 'order':
            self.expect_next_token(sqltokenizer.SqlTokenKind.KEYWORD, 'by')

            more_fields = True
            while more_fields:
                # Field name
                self.expect_next_token(sqltokenizer.SqlTokenKind.IDENTIFIER)
                order_by_val = self.val
                self.next_token()

                # Order by type
                if self.token == sqltokenizer.SqlTokenKind.KEYWORD and (self.val == 'asc' or self.val == 'desc'):
                    order_by_list.append((order_by_val, self.val))
                    self.next_token()
                else:
                    order_by_list.append((order_by_val, 'asc'))
                more_fields = self.token == sqltokenizer.SqlTokenKind.OPERATOR and self.val == ','

        self.expect_cur_token(sqltokenizer.SqlTokenKind.OPERATOR, ';')

        return NodeCommands.NodeSelect(field_list, outfile_name, table_name, where, group_by_list, having,
                                       order_by_list)

    # Parses the next command, returns a NodeCommand object with the necessary details
    def parse_command(self):
        self.next_token()

        if self.token == sqltokenizer.SqlTokenKind.EOF:  # no more commands
            return None
        self.expect_cur_token(sqltokenizer.SqlTokenKind.KEYWORD)

        if self.val == 'create':
            return self.parse_create()
        if self.val == 'drop':
            return self.parse_drop()
        if self.val == 'load':
            return self.parse_load()
        if self.val == 'select':
            return self.parse_select()

    # Raises a CSVDBSyntaxError Error
    def raise_error(self, message):
        raise CSVDBErrors.CSVDBSyntaxError(message, self.line, self.col, self.text)
