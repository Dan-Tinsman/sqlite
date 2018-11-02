
"""
Name: Daniel Tinsman

Sources:
    Class Lecture Video - Tokenize Functions
    https://docs.python.org/3/library/collections.html
    https://docs.python.org/3/library/functions.html#zip
    https://docs.python.org/3/howto/sorting.html
    
    
"""
import string
from collections import defaultdict
from operator import itemgetter

_ALL_DATABASES = {}
last_created_table = ""
last_created_cols = []
local_cols = []

"""
Below is a dictionary of dictionaries
The top level dictionary representing the Database,
and the next level representing the tables within
"""
database = defaultdict(dict)


def collect_characters(query, allowed_characters):
  letters = []
  for letter in query:
    if letter not in allowed_characters:
      break
    letters.append(letter)
  return "".join(letters)


def remove_leading_whitespace(query, tokens):
  whitespace = collect_characters(query, string.whitespace)
  return query[len(whitespace):]


def remove_word(query, tokens):
  word = collect_characters(query, string.ascii_letters + "_" + string.digits)
  tokens.append(word)
  return query[len(word):]
  

def remove_text(query, tokens):
    assert query[0] in ["'", '"']
    if query[0] == "'":
        query = query[1:]
        end_quote_index = query.find("'")
        text = query[:end_quote_index]
        tokens.append(text)
        query = query[end_quote_index + 1:]
    elif query[0] == '"':
        query = query[1:]
        end_quote_index = query.find('"')
        text = query[:end_quote_index]
        tokens.append(text)
        query = query[end_quote_index + 1:]
    
    return query


def remove_integer(query, tokens):
  int_str = collect_characters(query, string.digits)
  tokens.append(int_str)
  return query[len(int_str):]


def remove_number(query, tokens):
  query = remove_integer(query, tokens)
  if query[0] == ".":
    whole_str = tokens.pop()
    query = query[1:]
    query = remove_integer(query, tokens)
    frac_str = tokens.pop()
    float_str = whole_str + "." + frac_str
    tokens.append(float(float_str))
  else:
    int_str = tokens.pop()
    tokens.append(int(int_str))
  return query
  

def tokenize(query):
  tokens = []
  while query:
    old_query = query
    
    if query[0] in string.whitespace:
      query = remove_leading_whitespace(query, tokens)
      continue
    
    if query[0] in (string.ascii_letters + "_"):
      query = remove_word(query, tokens)
      continue
    
    if query[0] in "(),;*.<>=!":
      tokens.append(query[0])
      query = query[1:]
      continue
      
    if query[0] in ["'", '"']:
      query = remove_text(query, tokens)
      continue
    
    if query[0] in string.digits:
      query = remove_number(query, tokens)
      continue
    
    if len(query) == len(old_query):
      raise AssertionError("Query didn't get shorter.")
      
  return tokens
  

def remove_escaping(statement):
    
    if statement.count("'") == 0:
        return statement
        
    cols_o_paren = statement.find("(")
    cols_c_paren = statement.find(')') + 1
    
    cols1 = statement[cols_o_paren:cols_c_paren]
    
    cols1 = cols1.split(',')
    
    number_of_values = len(cols1)
    
    start_of_VALUES = statement.find("VALUES")
    
    open_paren = start_of_VALUES + 7
    close_paren = statement.rfind(')') + 1
    
    vals1 = statement[open_paren:close_paren]
    
    vals1 = vals1.replace('(', '')
    vals1 = vals1.replace(')', '')
    
    vals = vals1.split(',')
    
    for i,v in enumerate(vals):
        
        if v.count("'") == 0:
            continue
        
        first_quote = v.find("'")
        last_quote = v.rfind("'")
        
        quote = v[first_quote:last_quote] 
        found_quote = False
        result = ""
        for c in quote:
            if c == "'":
                if found_quote:
                    result += "'"
                    found_quote = False
                else:
                    found_quote = True
                continue
            result += c
        quote = result
        
        if quote.count("'") == 0:
            if quote[0] != '"' and quote[0] != "'":
                quote = "'" + quote + "'"
        elif quote.count("'") % 2 == 0:
            if quote.find("'") == 0 and quote.rfind("'") == -1:
                pass
            else:
                quote = '"' + quote + '"'
        else:
            quote = '"' + quote + '"'
        
        v = quote
        
        vals[i] = v
    
    vals[0] = '(' + vals[0]
    vals[-1] = vals[-1] + ')'
    
    for i in range(0, len(vals), number_of_values):
        if i == 0:
            continue
        vals[i] = '(' + vals[i]
        
    for i in range(number_of_values-1, len(vals)-1, number_of_values):
        vals[i] = vals[i] + ')'
    
    results = ", ".join(vals)

    vals1 = statement.replace(statement[open_paren:close_paren], results, 1)
    
    return vals1


def create_table(tokens):
    """
    Creates a table for the Database
    """
    global local_cols
    local_cols = []
    
    open_paren = tokens.index('(')
    close_paren = tokens.index(')')
    
    first_col_name = open_paren + 1
    
    for x in range(len(tokens[first_col_name:close_paren])):
        if tokens[first_col_name + x] == 'INTEGER':
            pass
        elif tokens[first_col_name + x] == 'REAL':
            pass
        elif tokens[first_col_name + x] == 'TEXT':
            pass
        elif tokens[first_col_name + x] == ',':
            pass
        else:
            local_cols.append(tokens[first_col_name + x])
            database[tokens[2]][tokens[first_col_name + x]] = []




def insert_multiple(tokens, values_as_tokens, cols_to_insert_to, table, insert_format):
    if insert_format == "uneven specific" or insert_format == "even unspecific":
        
        
        # number of value sets we need to add to table
        num_val_tups = values_as_tokens.count(')')
        
        for i in range(num_val_tups):
            
            vals = []   # vals to insert into cols
            left_out = []   # cols that need 'None' appended
        
            open_paren = values_as_tokens.index('(')
            close_paren = values_as_tokens.index(')')
            
            first_val_name = open_paren + 1
            
            # build the list of values from inside the VALUES ( )
            for x in range(len(values_as_tokens[first_val_name:close_paren])):
                if values_as_tokens[first_val_name + x] == ',':
                    pass
                    
                elif values_as_tokens[first_val_name + x] == 'NULL':
                    vals.append((None))
                
                else:
                    vals.append((values_as_tokens[first_val_name + x]))
            
            all_cols = list(database[table].keys())
            
            for col in all_cols:
                if col not in cols_to_insert_to:
                    left_out.append(col)
            
            # insert the values into the database table        
            for i in range(len(cols_to_insert_to)):
                database[table][cols_to_insert_to[i]].append(vals[i]) 
                
            for i in range(len(left_out)):
                database[table][left_out[i]].append(None) 
                
            # shorten the list to move on to the next value pair
            values_as_tokens = values_as_tokens[close_paren+1:]


def insert_into(tokens, last_created_table, last_created_cols):
    """
    Inserts values into table
    """
    table = tokens[2]
    
    # col order NOT specfied
    if tokens[3] == "VALUES":
        
        cols_to_insert_to = list(database[table].keys())
        
        short_tokens = tokens[tokens.index("VALUES") + 1:]
        
        if short_tokens.count(')') > 1:
            return insert_multiple(tokens, short_tokens, cols_to_insert_to, table, "even unspecific")
        
        vals = []
        
        open_paren = tokens.index('(')
        close_paren = tokens.index(')')
        
        first_val_name = open_paren + 1
        
        for x in range(len(tokens[first_val_name:close_paren])):
            if tokens[first_val_name + x] == ',':
                pass
                
            elif tokens[first_val_name + x] == 'NULL':
                vals.append((None))
            
            else:
                vals.append((tokens[first_val_name + x]))
        
       
            
        for j in range(len(cols_to_insert_to)):
            database[table][cols_to_insert_to[j]].append(vals[j])
    
    # col order IS specfied            
    else:
        
        cols_to_insert_to = []  # list of col order specfied
        
        
        open_paren = tokens.index('(')
        close_paren = tokens.index(')')
        
        first_col_name = open_paren + 1
        
        for x in range(len(tokens[first_col_name:close_paren])):
            if tokens[first_col_name + x] == ',':
                pass
            else:
                cols_to_insert_to.append((tokens[first_col_name + x]))
                
        
        # if num cols to insert to = num cols in table
        if len(cols_to_insert_to) == len(list(database[table].keys())):
            
            vals = []   # vals to insert into cols
            
            short_tokens = tokens[tokens.index("VALUES") + 1:]
            
            open_paren = short_tokens.index('(')
            close_paren = short_tokens.index(')')
            
            first_val_name = open_paren + 1
            
            # build the list of values from inside the VALUES ( )
            for x in range(len(short_tokens[first_val_name:close_paren])):
                if short_tokens[first_val_name + x] == ',':
                    pass
                    
                elif short_tokens[first_val_name + x] == 'NULL':
                    vals.append((None))
                
                else:
                    vals.append((short_tokens[first_val_name + x]))
            
            # insert the values into the database table        
            for i in range(len(cols_to_insert_to)):
                database[table][cols_to_insert_to[i]].append(vals[i])
        
        # if num cols to insert to does NOT EQUAL num cols in table      
        else:
            
            vals = []   # vals to insert into cols
            left_out = []   # cols that need 'None' appended
            
            short_tokens = tokens[tokens.index("VALUES") + 1:]
            
            
            
            
            # check for multiple tuples of VALUES
            # if so, call insert_multiple and return
            if short_tokens.count(')') > 1:
                return insert_multiple(tokens, short_tokens, cols_to_insert_to, table, "uneven specific")
                
                
                
            
            open_paren = short_tokens.index('(')
            close_paren = short_tokens.index(')')
            
            first_val_name = open_paren + 1
            
            # build the list of values from inside the VALUES ( )
            for x in range(len(short_tokens[first_val_name:close_paren])):
                if short_tokens[first_val_name + x] == ',':
                    pass
                    
                elif short_tokens[first_val_name + x] == 'NULL':
                    vals.append((None))
                
                else:
                    vals.append((short_tokens[first_val_name + x]))
            
            all_cols = list(database[table].keys())
            
            for col in all_cols:
                if col not in cols_to_insert_to:
                    left_out.append(col)
            
            # insert the values into the database table        
            for i in range(len(cols_to_insert_to)):
                database[table][cols_to_insert_to[i]].append(vals[i]) 
                
            for i in range(len(left_out)):
                database[table][left_out[i]].append(None) 
        
            
            
def order_by(tokens, zipped_select):
    
    # find the point in tokens where the FROM command occurs
    _select = tokens.index("SELECT")
    select_col_lst = []     # list of selected cols
    from_table = tokens[tokens.index("FROM") + 1]   # SELECT FROM table
    order_by_cols = []      # list of cols to order by
    sored_key_lst = []      # list of keys to sort by
    num_order_cols = 0      # number of cols we need to order by
    
    
    # Loop through the remainder of the tokens and append each SELECT col
    for token in tokens[_select + 1:]:
        if token == "FROM":
            break
        elif token == "DISTINCT":
            continue
        elif '*' in token:
            for col in list(database[from_table].keys()):
                select_col_lst.append(col)
        elif token not in ',;':
            select_col_lst.append(token)
    
    # find the point in tokens where the ORDER BY command occurs
    order = tokens.index("ORDER")
    if tokens[order + 1] == "BY":
        order_by_1 = tokens[order + 2]
    
    # Loop through the remainder of the tokens and append each ORDER BY col
    for token in tokens[order + 2:]:
        if token not in ',;':
            order_by_cols.append(token)
            
    # update num_order_cols
    num_order_cols = len(order_by_cols)
    
    # if there is nothing in the table, because it was deleted
    # reuturn []
    if len(select_col_lst) == 0:
        return []
    
    # going to convert them all to qualified_tokens
    for x, col in enumerate(order_by_cols):
        if '.' not in col:
            order_by_cols[x] = from_table + '.' + col
            
    # going to convert them all to qualified_tokens
    for x, col in enumerate(select_col_lst):
        if '.' not in col:
            select_col_lst[x] = from_table + '.' + col
       
    for i in range(num_order_cols):
       
        sorted_by = order_by_cols[i]
        indx = select_col_lst.index(sorted_by)
       
        sored_key_lst.append(indx)    
            
    sorted_ = sorted(zipped_select, key=itemgetter(*sored_key_lst))
    
    return sorted_
    
def select_from(tokens):
    # find the point in tokens where the FROM command occurs
    _select = tokens.index("SELECT")
    # find the point in tokens where the FROM command occurs
    _from_table = tokens[tokens.index("FROM") + 1]
    
    select_col_lst = []     # list of selected cols
    num_select_cols = 0     # lengh of list of selected cols
    
    result_lst = []

    if tokens[_select + 1] == '*' and tokens[_select + 2] == "FROM":
        everything = zip(*database[_from_table].values())
        return everything
        
    else:
    
        # Loop through the remainder of the tokens and append each SELECT col
        for token in tokens[_select + 1:]:
            if token == "FROM":
                break
            elif token == "DISTINCT":
                continue
            elif token not in ',;':
                select_col_lst.append(token)
            
        # update num_select_cols
        num_select_cols = len(select_col_lst)
        
        # selected col results in for loop
        selected_col_res = ""
        col = ""
        
        # going to convert them all to qualified_tokens
        for x, col in enumerate(select_col_lst):
            if '.' not in col:
                select_col_lst[x] = _from_table + '.' + col
                
        # for every col in select_col_lst  
        for i in range(num_select_cols):
            period_index = select_col_lst[i].find('.')
            qualified_table = select_col_lst[i][:period_index]
            qualified_col = select_col_lst[i][period_index + 1:]
            
            q_col = select_col_lst[i].index('.')
            col = select_col_lst[i][q_col + 1:]
        
            if col == '*':
                everything = list(database[_from_table].values())
                for j in range(len(everything)):
                    selected_col_res = everything[j]
                    result_lst.append(selected_col_res)
                continue
            
            else:
                selected_col_res = database[_from_table][col]
                result_lst.append(selected_col_res)
        
        res_lst = zip(*result_lst)
        
        if "WHERE" not in tokens:
            if "DISTINCT" in tokens:
                res_lst = select_distinct(tokens, res_lst)
                
        return res_lst
        
def select_where(tokens):
    
    
    # find SELECT DISTINCT _ FROM
    if "DISTINCT" in tokens:
        _distinct_from = tokens[tokens.index("DISTINCT")+1]
        
    # find regular SELECT _ FROM
    else:
        _select_from = tokens[tokens.index("SELECT")+1]
    
    # find SELECT keyword
    _select = tokens.index("SELECT")
    
    # list of cols we need to select
    select_col_lst = []
    
    # find the TABLE name
    _from_table = tokens[tokens.index("FROM") + 1]
    
    # list of final results we will build
    where_list = []
    
    # find the index of "WHERE"
    where_indx = tokens.index("WHERE")
    
    # create a list of each col in the TABLE
    all_cols = list(database[_from_table].keys())
    
    # find the specified WHERE col
    where_col = tokens[where_indx+1]
    
    if '.' in where_col:
        where_col = where_col[where_col.find('.')+1:]
    
    num_select_cols = 0
    where_col_tup_indx = 0
    
    result_lst = []
    
    # Loop through the remainder of the tokens and append each SELECT col
    for token in tokens[_select + 1:]:
        if token == "FROM":
            break
        elif token == "DISTINCT":
            continue
        elif token not in ',;':
            select_col_lst.append(token)
    
    if tokens[_select + 1] == '*' and tokens[_select + 2] == "FROM":
        everything = zip(*database[_from_table].values())
        result_list = everything
        
        # find the relative tuple index of the specified WHERE col
        where_col_tup_indx = all_cols.index(where_col)
        
    elif '*' not in tokens[_select + 1] and tokens[_select + 2] == "FROM":
        everything = zip(*database[_from_table].values())
        result_list = everything
        
        # find the relative tuple index of the specified WHERE col
        where_col_tup_indx = all_cols.index(where_col)
        
    elif '*' not in tokens[_select + 2] and tokens[_select + 3] == "FROM":
        everything = zip(*database[_from_table].values())
        result_list = everything
        
        # find the relative tuple index of the specified WHERE col
        where_col_tup_indx = all_cols.index(where_col)
        
    else:
    
        # update num_select_cols
        num_select_cols = len(select_col_lst)
        
        # selected col results in for loop
        selected_col_res = ""
        col = ""
        
        # going to convert them all to qualified_tokens
        for x, col in enumerate(select_col_lst):
            if '.' not in col:
                select_col_lst[x] = _from_table + '.' + col
                
        total_col_list = []
                
        # for every col in select_col_lst  
        for i in range(num_select_cols):
            period_index = select_col_lst[i].find('.')
            qualified_table = select_col_lst[i][:period_index]
            qualified_col = select_col_lst[i][period_index + 1:]
            
            q_col = select_col_lst[i].index('.')
            col = select_col_lst[i][q_col + 1:]
        
            if col == '*':
                for x in list(database[_from_table].keys()):
                    total_col_list.append(x)
                everything = list(database[_from_table].values())
                for j in range(len(everything)):
                    selected_col_res = everything[j]
                    result_lst.append(selected_col_res)
                continue
            
            else:
                total_col_list.append(col)
                selected_col_res = database[_from_table][col]
                result_lst.append(selected_col_res)
            
        # find the relative tuple index of the specified WHERE col
        where_col_tup_indx = total_col_list.index(where_col)
            
        result_list = zip(*result_lst)
    
    
    # check if it is a qualified name or not
    if '.' in where_col:
        # find the col name separate from table
        _from_table = where_col[:where_col.find('.')]
       
        where_col = where_col[where_col.find('.')+1:]
    
    # find the specified col operator
    col_operator = tokens[where_indx+2]
    
    if tokens[where_indx+3] == "NOT":
        col_operator = "NOT"

    # find the specified value to compare to
    if col_operator == '!':
        comp_val = tokens[where_indx+4]
    else:
        comp_val = tokens[where_indx+3]
    
    if comp_val == "NULL":
        comp_val = None

    # shorten the tokens to work with the "WHERE" statement
    where_tokens = tokens[where_indx:]
    
   
    if col_operator == '>':
        for tup in result_list:
            if tup[where_col_tup_indx] == None:
                pass
            elif tup[where_col_tup_indx] > comp_val:
                where_list.append(tup)
                
    elif col_operator == '<':
        for tup in result_list:
            if tup[where_col_tup_indx] == None:
                pass
            elif tup[where_col_tup_indx] < comp_val:
                where_list.append(tup)
                
    elif col_operator == '=':
        for tup in result_list:
            if tup[where_col_tup_indx] == comp_val:
                where_list.append(tup)
                
    elif col_operator == "IS":
        for tup in result_list:
            if tup[where_col_tup_indx] is comp_val:
                where_list.append(tup)
                
    elif col_operator == "NOT":
        comp_val = tokens[tokens.index(comp_val) + 1]
        if comp_val == "NULL":
            comp_val = None
        for tup in result_list:
            if tup[where_col_tup_indx] is not comp_val:
                where_list.append(tup)
                
    elif col_operator == "!":
        for tup in result_list:
            if tup[where_col_tup_indx] == None:
                pass
            elif tup[where_col_tup_indx] != comp_val:
                where_list.append(tup)
        
    if "DISTINCT" in tokens: 
        if _distinct_from == '*':
            return select_distinct(tokens, where_list)
        else:
            new_answer = []
            
            for i, col in enumerate(select_col_lst):
                
                indx = all_cols.index(col)
       
                for tup in where_list:
                    new_answer.append(tup[indx])
            
            # using list comprehension to change a list of elements into a list of tuples as expected by the order_by function
            new_answer = [(i,) for i in new_answer]
                    
            return new_answer
            
    else:
        return where_list

    
def select_distinct(tokens, res_lst):
    
    # res_lst is the selected values without checking for duplicates
    # now we will check for and remove duplicates using sets
    
    # create a set out of the res_lst
    res_lst = set(res_lst)
    
    # convert the res_lst back into a list
    res_lst = list(res_lst)
    
    return res_lst
    
    
def update(tokens):
    
    # find the table we are updating
    table_to_update = tokens[tokens.index("UPDATE") + 1]
    
    # find the number of cols we are updating
    # which is equal to the number of commas in the statement + 1
    num_cols_to_update = tokens[tokens.index("SET"):].count(',') + 1
    
    # create a list of the cols and vals to update and populate it
    cols_vals_to_update = []
    
    # create a shorter version of the tokens
    set_tokens = []
    
    if "WHERE" in tokens:
        set_tokens = tokens[tokens.index("SET"):tokens.index("WHERE")+1]
    else:
        set_tokens = tokens[tokens.index("SET"):]
        
    for token in set_tokens:
        if token in ["SET", ',', '=']:
            continue
        elif token in ["WHERE", ';']:
            break
        else:
            cols_vals_to_update.append(token)
         
    # make a list of the cols to update
    # start with combined list and keep every other starting with 0th elem
    cols_to_update = cols_vals_to_update[::2]  

    # make a list of the vals to update
    # start with combined list and keep every other starting with 1st elem
    vals_to_update = cols_vals_to_update[1::2] 
    
    # no WHERE statement to process
    if "WHERE" not in tokens:
    
        # should always be a 1:1 relation so for each col, I can update
        # the according val using enumerate
        for i, col in enumerate(cols_to_update):
            for j in range(len(database[table_to_update][col])):
                database[table_to_update][col][j] = vals_to_update[i]
    
    # have to process a WHERE statement            
    else:
        
        # find the index of "WHERE"
        where_indx = tokens.index("WHERE")
        
        # find the specified WHERE col
        where_col = tokens[where_indx+1]
    
        # check if it is a qualified name or not
        if '.' in where_col:
            # find the col name separate from table
            _from_table = where_col[:where_col.find('.')]
           
            where_col = where_col[where_col.find('.')+1:]
        
        # find the specified col operator
        col_operator = tokens[where_indx+2]
    
        if tokens[where_indx+3] == "NOT":
            col_operator = "NOT"
            
         # find the specified value to compare to
        if col_operator == '!':
            comp_val = tokens[where_indx+4]
        else:
            comp_val = tokens[where_indx+3]
        
        if comp_val == "NULL":
            comp_val = None
        
        
        for i, col in enumerate(cols_to_update):
            for j in range(len(database[table_to_update][col])):
                
                if col_operator == '>':
                    if database[table_to_update][where_col][j] > comp_val:
                        database[table_to_update][col][j] = vals_to_update[i]
                        
                elif col_operator == '<':
                    if database[table_to_update][where_col][j] < comp_val:
                        database[table_to_update][col][j] = vals_to_update[i]
                            
                elif col_operator == '=':
                    if database[table_to_update][where_col][j] == comp_val:
                        database[table_to_update][col][j] = vals_to_update[i]
                            
                elif col_operator == "IS":
                    if database[table_to_update][where_col][j] is comp_val:
                        database[table_to_update][col][j] = vals_to_update[i]
                            
                elif col_operator == "NOT":
                    if database[table_to_update][where_col][j] is not comp_val:
                        database[table_to_update][col][j] = vals_to_update[i]
                            
                elif col_operator == "!":
                    if database[table_to_update][where_col][j] != comp_val:
                        database[table_to_update][col][j] = vals_to_update[i]
               
                        
    
    
    
def delete(tokens):
    
    # find the table to delete from
    table_to_delete = tokens[tokens.index("FROM")+1]
    
    # check first to see if there is a WHERE statement
    if "WHERE" in tokens:
        
        # find the index of "WHERE"
        where_indx = tokens.index("WHERE")
        
        # find the specified WHERE col
        where_col = tokens[where_indx+1]
    
        # check if it is a qualified name or not
        if '.' in where_col:
            # find the col name separate from table
            table_to_delete = where_col[:where_col.find('.')]
           
            where_col = where_col[where_col.find('.')+1:]
        
        # find the specified col operator
        col_operator = tokens[where_indx+2]
    
        if tokens[where_indx+3] == "NOT":
            col_operator = "NOT"
            
         # find the specified value to compare to
        if col_operator == '!':
            comp_val = tokens[where_indx+4]
        else:
            comp_val = tokens[where_indx+3]
        
        if comp_val == "NULL":
            comp_val = None
            
        # shorten the tokens to work with the "WHERE" statement
        where_tokens = tokens[where_indx:]
        
        # create a list of each col in the TABLE
        all_cols = list(database[table_to_delete].keys())
        all_vals = list(database[table_to_delete].values())
        #cols_to_select = 
        
        # find the relative tuple index of the specified WHERE col
        where_col_tup_indx = all_cols.index(where_col)
        
        indexes_to_delete = []   
        
        if col_operator == "IS":
            indexes_to_delete = []
            for x,ele in enumerate(database[table_to_delete][where_col]):
                if ele is comp_val:
                    indexes_to_delete.append(x)
                    
            for i,col in enumerate(all_cols):
                database[table_to_delete][col] = [i for j, i in enumerate(database[table_to_delete][col]) if j not in indexes_to_delete]
                
        elif col_operator == '<':
            indexes_to_delete = []
            for x,ele in enumerate(database[table_to_delete][where_col]):
                if ele == None:
                    pass
                elif ele < comp_val:
                    indexes_to_delete.append(x)
                    
            for i,col in enumerate(all_cols):
                database[table_to_delete][col] = [i for j, i in enumerate(database[table_to_delete][col]) if j not in indexes_to_delete]
                
        elif col_operator == '>':
            indexes_to_delete = []
            for x,ele in enumerate(database[table_to_delete][where_col]):
                if ele == None:
                    pass
                elif ele > comp_val:
                    indexes_to_delete.append(x)
                    
            for i,col in enumerate(all_cols):
                database[table_to_delete][col] = [i for j, i in enumerate(database[table_to_delete][col]) if j not in indexes_to_delete]
                
        elif col_operator == '=':
            indexes_to_delete = []
            for x,ele in enumerate(database[table_to_delete][where_col]):
                if ele == comp_val:
                    indexes_to_delete.append(x)
                    
            for i,col in enumerate(all_cols):
                database[table_to_delete][col] = [i for j, i in enumerate(database[table_to_delete][col]) if j not in indexes_to_delete]
                
        elif col_operator == "NOT":
            indexes_to_delete = []
            for x,ele in enumerate(database[table_to_delete][where_col]):
                if ele is not comp_val:
                    indexes_to_delete.append(x)
                    
            for i,col in enumerate(all_cols):
                database[table_to_delete][col] = [i for j, i in enumerate(database[table_to_delete][col]) if j not in indexes_to_delete]
                
        elif col_operator == '!':
            indexes_to_delete = []
            for x,ele in enumerate(database[table_to_delete][where_col]):
                if ele != comp_val:
                    indexes_to_delete.append(x)
                    
            for i,col in enumerate(all_cols):
                database[table_to_delete][col] = [i for j, i in enumerate(database[table_to_delete][col]) if j not in indexes_to_delete]
            
                            
            
        
            
    
    # If not, just delete everything
    else:
        # if DELETE FROM 
        if tokens[0] == "DELETE" and tokens[1] == "FROM":
            del database[table_to_delete]
 

def left_outer_join(tokens):
    
    # left of join
    a_indx = tokens.index("FROM") + 1
    
    # right of join
    b_indx = tokens.index("JOIN") + 1
    
    # left of =
    on_l_indx = tokens.index("=") - 1
    
    # right of = 
    on_r_indx = tokens.index("=") + 1 
    
    l_table = tokens[on_l_indx]
    l_table = l_table[:l_table.find('.')]
    
    r_table = tokens[on_r_indx]
    r_table = r_table[:r_table.find('.')]
    
    l_col = tokens[on_l_indx]
    l_col = l_col[l_col.find('.')+1:]
    
    r_col = tokens[on_r_indx]
    r_col = r_col[r_col.find('.')+1:]
    
    
    # shorten the tokens to SELECT:FROM to find table_a cols
    table_a_cols = []
    table_b_cols = []
    all_table_cols_q = []
    correct_order = []
    cnt = 0
    for token in tokens[1:tokens.index("FROM")]:
        if token != ',' and token != ')' and token != "SELECT":
            all_table_cols_q.append(token)
        if token != ',' and tokens[a_indx] in token:
            table_a_cols.append(token)
            correct_order.append(cnt)
            cnt += 1
        elif token != ',' and tokens[b_indx] in token:
            table_b_cols.append(token)
            correct_order.append(cnt)
            cnt += 1
            
   
            
    # create the list of table_a cols without qualified table      
    for i,col in enumerate(table_a_cols):
        table_a_cols[i] = col[col.find('.')+1:]
        
    # create the list of table_b cols without qualified table      
    for i,col in enumerate(table_b_cols):
        table_b_cols[i] = col[col.find('.')+1:]
        
    # create the list of all table cols without qualified table  
    all_table_cols = []
    for i,col in enumerate(all_table_cols_q):
        all_table_cols.append(col[col.find('.')+1:])
     
    
    # call select_from to get table_a
    table_a = select_from(tokenize("SELECT * FROM " + tokens[a_indx]))
    table_a = list(table_a)
    table_a = [list(elem) for elem in table_a]
    
    # call select_from to get table_b 
    table_b = select_from(tokenize("SELECT * FROM " + tokens[b_indx]))
    table_b = list(table_b)
    table_b = [list(elem) for elem in table_b]



    all_right_table_cols = list(database[r_table].keys())
    all_left_table_cols = list(database[l_table].keys())
    
    tbl_a_col_indx = 0


    if tokens[a_indx] == l_table:
        #print("table_a is the same as left of equal", tokens[a_indx], l_table)
        tbl_a_col_indx = all_left_table_cols.index(l_col)
    else:
        #print("table_b is the same as left of equal", tokens[b_indx], l_table)
        tbl_a_col_indx = all_right_table_cols.index(r_col)
    
    res_table = []
    for i,row_a in enumerate(table_a):
        for row_b in table_b:
            for ele in row_a:
                if ele in row_b:
                    if ele == None:
                        
                        res_table.append(row_a+[None]*len(row_b))
                    else:
                        res_table.append(row_a+row_b)
                
    
    res_lst_eles = []
    for row in res_table:
        for ele in row:
            res_lst_eles.append(ele)
            
            
    #print(table_a, tbl_a_col_indx)
    for tup in table_a:
        if tup[tbl_a_col_indx] in res_lst_eles:
            #print("yes",tup[tbl_a_col_indx] )
            pass
        else:
            #print("no", tup[tbl_a_col_indx])
            res_table.append(tup + [None]*(len(res_table[0]) - len(tup)))
    
    #res_table = set(res_table)
    res_table = set(tuple(row) for row in res_table)
    #return res_table
        
    
    if "WHERE" in tokens:
        
        if tokens[a_indx] == l_table: 
            every_single_col = all_left_table_cols + all_right_table_cols
        else:
            every_single_col = all_right_table_cols + all_left_table_cols
        
        where_list = []
        # find the index of "WHERE"
        where_indx = tokens.index("WHERE")
        
        # find the specified WHERE col
        where_col = tokens[where_indx+1]
    
        # check if it is a qualified name or not
        if '.' in where_col:
            # find the col name separate from table
            _from_table = where_col[:where_col.find('.')]
           
            where_col = where_col[where_col.find('.')+1:]
        
        # find the specified col operator
        col_operator = tokens[where_indx+2]
    
        if tokens[where_indx+3] == "NOT":
            col_operator = "NOT"
            
         # find the specified value to compare to
        if col_operator == '!':
            comp_val = tokens[where_indx+4]
        else:
            comp_val = tokens[where_indx+3]
        
        if comp_val == "NULL":
            comp_val = None
            
        # shorten the tokens to work with the "WHERE" statement
        where_tokens = tokens[where_indx:]
        
        # create a list of each col in the TABLE
        all_cols = list(database[_from_table].keys())
        
        # find the relative tuple index of the specified WHERE col
        where_col_tup_indx = every_single_col.index(where_col)
        
        
        if col_operator == '>':
            for tup in res_table:
                if tup[where_col_tup_indx] == None:
                    pass
                elif tup[where_col_tup_indx] > comp_val:
                    where_list.append(tup)
                    
        elif col_operator == '<':
            for tup in res_table:
                if tup[where_col_tup_indx] == None:
                    pass
                elif tup[where_col_tup_indx] < comp_val:
                    where_list.append(tup)
                    
        elif col_operator == '=':
            for tup in res_table:
                if tup[where_col_tup_indx] == comp_val:
                    where_list.append(tup)
                    
        elif col_operator == "IS":
            for tup in res_table:
                if tup[where_col_tup_indx] is comp_val:
                    where_list.append(tup)
                    
        elif col_operator == "NOT":
            comp_val = tokens[tokens.index(comp_val) + 1]
            if comp_val == "NULL":
                comp_val = None
            for tup in res_table:
                if tup[where_col_tup_indx] is not comp_val:
                    where_list.append(tup)
                    
        elif col_operator == "!":
            for tup in res_table:
                if tup[where_col_tup_indx] == None:
                    pass
                elif tup[where_col_tup_indx] != comp_val:
                    where_list.append(tup)
        
        res_table = where_list
      
    for i,col in enumerate(all_left_table_cols):
        #all_left_table_cols[i] = col + '.' + l_table
        all_left_table_cols[i] = l_table + '.' + col
    
     
    for i,col in enumerate(all_right_table_cols):
        #all_right_table_cols[i] = col + '.' + r_table
        all_right_table_cols[i] = r_table + '.' + col
      
        
       
    if tokens[a_indx] == l_table: 
        every_single_col = all_left_table_cols + all_right_table_cols
    else:
        every_single_col = all_right_table_cols + all_left_table_cols
    
    
    for i,col in enumerate(table_a_cols):
        #table_a_cols[i] = col + '.' + tokens[a_indx]
        table_a_cols[i] = tokens[a_indx] + '.' + col
     
    for i,col in enumerate(table_b_cols):
        #table_b_cols[i] = col + '.' + tokens[b_indx]
        table_b_cols[i] = tokens[b_indx] + '.' + col
    
    
    
    needed_cols = table_a_cols + table_b_cols
    
    remove = []
    keep = []
    for i,ele in enumerate(every_single_col):
        if ele not in all_table_cols_q:
            remove.append(i)
        else:
            keep.append(i)
            
    order = []
    for ele in all_table_cols_q:
        order.append(every_single_col.index(ele))
            
    
    res_table = list(res_table)
    for i,row in enumerate(res_table):
        res_table[i] = [row[x] for x in order]
        
    # change from list of lists back into list of tuples
    res_table = list(set(tuple(row) for row in res_table))
        
        
    res_table = [tuple(x) for x in res_table]
    
    if "ORDER" in tokens:
        
        return order_by(tokens, res_table)

    return res_table
    

class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        pass

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
     
        # send to function that gets rid of escaping collect_characters
        without_escapes = remove_escaping(statement)
        
        # send statement to tokenize function
        tokens = tokenize(without_escapes)
        
        # find each period index in tokens list and insert into period_list
        period_list = [i for i,x in enumerate(tokens) if x=="."]
        
        # list of the qualified names that are to replace the periods in tokens
        q_name_list = []    # list of qualified names
        
        # going to combine either side of the period for qualified names
        for period in period_list:
            qualified_token = tokens[period - 1] + tokens[period] + tokens[period + 1]
            
            q_name_list.append(qualified_token)
        
        for x in range(len(period_list)):
            next_period = tokens.index('.')
            tokens[next_period] = q_name_list[x]
            del tokens[next_period-1]
            del tokens[next_period]
        
        open_paren = None
        close_paren = None
        
        if '(' in tokens:
            open_paren = tokens.index('(')
            first_col_name = open_paren + 1
        if ')' in tokens:
            close_paren = tokens.index(')')
        
        
        if "CREATE" in tokens:
            global last_created_table
            last_created_table = tokens[2]
            for x in range(len(tokens[first_col_name:close_paren])):
                if tokens[first_col_name + x] == 'INTEGER':
                    pass
                elif tokens[first_col_name + x] == 'REAL':
                    pass
                elif tokens[first_col_name + x] == 'TEXT':
                    pass
                elif tokens[first_col_name + x] == ',':
                    pass
                else:
                    last_created_cols.append(tokens[first_col_name + x])
                    
            create_table(tokens)
            return None
            
        elif "INSERT" in tokens:
            
            insert_into(tokens, last_created_table, last_created_cols)
            return None
            
        elif "SELECT" in tokens and "JOIN" in tokens:
            return left_outer_join(tokens)
            
        elif "SELECT" in tokens and "WHERE" not in tokens:
            
            res_lst = select_from(tokens)
                
            if "ORDER" in tokens and "BY" in tokens:
                
                return order_by(tokens, res_lst)
                
            else:
                return res_lst
                
        elif "SELECT" in tokens and "WHERE" in tokens:
                
            res_lst = select_where(tokens)
            
            if "ORDER" in tokens and "BY" in tokens:
                
                return order_by(tokens, res_lst)
                
            else:
                return res_lst
            
        elif "UPDATE" in tokens:
            return update(tokens)
            
        elif "DELETE" in tokens:
            return delete(tokens)
            
        else:
            pass
        
        return None
    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)
    
    
    
### TESTING ###

# from pprint import pprint

# def check(sql_statement, expected):
#   print("SQL: " + sql_statement)
#   result = conn.execute(sql_statement)
#   result_list = list(result)
  
#   print("expected:")
#   pprint(expected)
#   print("student: ")
#   pprint(result_list)
#   assert expected == result_list
  


# conn = connect("test.db")
# conn.execute("CREATE TABLE pets (name TEXT, species TEXT, age INTEGER);")
# conn.execute("CREATE TABLE owners (name TEXT, age INTEGER, id INTEGER);")
# conn.execute("INSERT INTO pets VALUES ('RaceTrack', 'Ferret', 3), ('Ghost', 'Ferret', 2), ('Zoe', 'Dog', 7), ('Ebony', 'Dog', 17);")
# conn.execute("INSERT INTO pets (species, name) VALUES ('Rat', 'Ginny'), ('Dog', 'Balto'), ('Dog', 'Clifford');")

# conn.execute("UPDATE pets SET age = 15 WHERE name = 'RaceTrack';")

# conn.execute("INSERT INTO owners VALUES ('Josh', 29, 10), ('Emily', 27, 2), ('Zach', 25, 4), ('Doug', 34, 5);")

# conn.execute("DELETE FROM owners WHERE name = 'Doug';")

# conn.execute("CREATE TABLE ownership (name TEXT, id INTEGER);")
# conn.execute("INSERT INTO ownership VALUES ('RaceTrack', 10), ('Ginny', 2), ('Ghost', 2), ('Zoe', 4);")


# check("SELECT pets.name, pets.age, ownership.id FROM pets LEFT OUTER JOIN ownership ON pets.name = ownership.name WHERE ownership.id = 2 ORDER BY pets.name;",
#   [('Ghost', 2, 2), ('Ginny', None, 2)]

#   )
