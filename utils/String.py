import re
from utils.Exceptions import InvalidArgumentToFunction

class String:
    """ This class provides all the utils functions for strings. """

    """ Rule to remove all text inside a comment in each language (default: Standard SQL (//, --, /**/)) """
    comments_patterns = {
        'standard_sql': r'//.*|--.*|\/\*.*?\*\/'
    }
    
    """ REGEX to identify a table with the pattern <project>.<dataset>.<table>"""
    tables_pattern = r"[\w'\"`_-]+\.[\w'\"`_-]+\.[\w'\"`_-]+"

    def remove_chars_from_string(self, string: str, chars_to_remove: list[str]) -> str:
        """Removes some special characters from a given string.

        Parameters
        ----------
        string : str
            A string with text

        chars_to_remove: list[str]
            List of chars to remove from the given string

        Returns
        -------
        str
            The same string with no more the selected chars.
        """
        if string is None or not isinstance(chars_to_remove, list) or len(chars_to_remove) == 0:
            raise InvalidArgumentToFunction()

        return re.sub('[' + ''.join(chars_to_remove) + ']', '', string)

    def remove_comments_from_string(self, string: str, dialect: str = 'standard_sql') -> str:
        """Removes all comments (//, --, /**/) (and the text inside) from a given text string.

        Parameters
        ----------
        string : str
            A text with a query
        dialect: str
            Each language has its own coding rule for comments. Default Standard SQL.

        Returns
        -------
        string:
            The same text with no more comments.
        """
        if string is None:
            raise InvalidArgumentToFunction()
        return re.sub(self.comments_patterns[dialect], '', string)

    def extract_tables_from_query(self, string: str) -> list[str]:
        """Extract all source tables from a query in a string.

        Parameters
        ----------
        string:
            Input query written in Standard SQL

        Returns
        -------
        List[str]
            A list with all sources tables.
        """
        if string is None:
            raise InvalidArgumentToFunction()
        
        """ Clear the input query, removing all comments and special chars """
        clear_query_from_comments             = self.remove_comments_from_string(string)
        clear_query_from_comments_and_symbols = re.sub("[^a-zA-Z0-9._\\s-]", "", clear_query_from_comments)

        """ Find all occurrences of the pattern inside the query """
        matches = re.findall(self.tables_pattern, clear_query_from_comments_and_symbols)

        """ Remove duplicates with set() """
        return list(set(matches))
 
    