#!/usr/local/bin/has-python -O
"""
Utilities to encode and serialize
"""
import re

class EscapedNewlineFilter:
    """
    Filter input for CSV reader.
    This removes null and escaped newline
    """
    def __init__(self, file):
        self.file = file

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def next(self):
        l = self.file.next()
        l = l.replace("\0", "")
        while len(l) > 1 and l[-2] == '\\' and (len(l) == 2 or l[-3] != '\\'):
            # Do not trim '\\' and '\n'.
            l = l + self.file.next()
        return l
    def close(self):
        self.file.close()

def _replacer(match):
    """
    A callback from CsvSerializer.escape_column
    """
    if match.group(0) == '\0':
        return ''
    else:
        return '\\' + match.group(0)

class CsvSerializer:
    """
    The output uses ',' as field separator and quote columns that would need one.
    This escapes back slashes, double quotes and new lines with back slashes.
    """
    ESCAPES = ['\\', "'", '"', '\r', '\n']
    QUOTABLE = frozenset([' ', '\t', ',', '\\', "'", '"', '\r', '\n'])
    QUOTABLE_PATTERN = re.compile(r"""[ ,\t\\'"\r\n]""")
    REMOVABLE = ['\0']
    VALIDATION_PATTERN = re.compile(
        # Detect invalid utf-8 encoding beyond 4 bytes sequence. Also detect \xef\xbf\xbf (U+FFFF) and
        # surrogate \xed\xa0\x80 to \xed\xbf\xbf or \ud800 to \udfff.
        u"[\ud800-\udfff\uffff]")
    VALIDATION_16_PATTERN = re.compile(
        # Detect invalid utf-8 encoding beyond 3 bytes sequence. Also detect \xef\xbf\xbf (U+FFFF) and
        # surrogate \xed\xa0\x80 to \xed\xbf\xbf or \ud800 to \udfff.
        u"[\ud800-\udfff\uffff\U00010000-\U0001ffff]")
    def __init__(self, utf8_sanitize, null_value='', separator=',', escapes=None, quotes='"', utf16_only=False):
        """ utf16_only will trim 4bytes utf-8 sequence or unicode codepoint U+10000 and above."""
        self.utf8_sanitize = utf8_sanitize
        self.null_value = null_value
        self.separator = separator
        self.escapes = escapes if escapes else self.ESCAPES
        self.escapes_removable_pattern = re.compile('[%s]' % ''.join(['\\\\' if e == '\\' else e for e in self.escapes] + ['\0']))
        self.validation_pattern = self.VALIDATION_16_PATTERN if utf16_only else self.VALIDATION_PATTERN
        self.quotes = quotes

    def escape_column(self, column):
        """
        Escape a column
        """
        if column is None:
            return self.null_value
        if not isinstance(column, (unicode, str)):
            return str(column)
        if isinstance(column, unicode):
            column = column.encode('utf-8')
        column = self.sanitize(column)

        needs_quote = self.QUOTABLE_PATTERN.search(column)

        # Escape the input and remove NULL.
        escaped = self.escapes_removable_pattern.sub(_replacer, column)

        if needs_quote:
            return self.quotes + escaped + self.quotes
        else:
            return escaped

    def serialize(self, row):
        """
        Generates a CSV string given a row
        """
        escaped = [self.escape_column(column) for column in row]
        return self.separator.join(escaped) + '\n'

    def sanitize(self, in_chars):
        """
        Sanitize input for invalid utf8
        """
        if not in_chars or not self.utf8_sanitize:
            return in_chars
        try:
            result, count = re.subn(self.validation_pattern, '', in_chars.decode('utf-8'))
            if count:
                result = result.encode('utf-8')
            else:
                result = in_chars
        except UnicodeError, e:
            result = self.sanitize(in_chars[:e.start] + in_chars[e.end:])
        return result

# Unit test stuff
class Test():
    """ Conducts unit tests. """
    def verify(self, result, case):
        """
        Utility to verify a test case
        """
        if result != case[1]:
            print ("Error: input: %s expected %s(%d) actual %s(%d)" % (
                    case[0], case[1], len(case[1]), result, len(result)))

    def test_csv_serializer(self):
        """
        Test escaping and sanitization
        """
        escape_cases = [
            # Use \x5c instead of \\ to avoid python string escaping.
            [['ab', '20', 'c d'], 'ab,20,"c d"\n'],
            [['a ', 'c\t', 'e\n'], '"a ","c\t","e\x5c\n"\n'],
            [['a b', 'c\td', 'e\nf'], '"a b","c\td","e\x5c\nf"\n'],
            [['"', '""', '"""'], '"\x5c"","\x5c"\x5c"","\x5c"\x5c"\x5c""\n'],
            [["'", "''", "'''"], """"\x5c'","\x5c'\x5c'","\x5c'\x5c'\x5c'"\n"""],
            [['\x5c', '\x5c\x5c'], '"\x5c\x5c","\x5c\x5c\x5c\x5c"\n'],
            [['\x5c"', '\x5c\x5c"', '\x5c""'], '"\x5c\x5c\x5c"","\x5c\x5c\x5c\x5c\x5c"","\x5c\x5c\x5c"\x5c""\n']]
        utf8_cases = [
            # Bad second byte of 2 bytes sequence.
            [['\xc3'], '\n'],
            [['\xc3 '], '" "\n'],
            # Bad or missing second byte after valid utf-8.
            [['\xe5\xae\x89\x8d'], '\xe5\xae\x89\n'],
            [['\xe5\xae\x89\x8d '], '"\xe5\xae\x89 "\n'],
            # Bad or missing bytes intermixed in valid utf-8
            [['\x91\xea'], '\n'],
            [['\x90\xea\xae'], '\n'],
            [['\x90\x24\x90'], '\x24\n'],
            [['\x90 \x45\xae'], '" \x45"\n'],
            [['\x91 \x90 '], '"  "\n'],
            [['\xe5\x91 \x90 '], '"  "\n'],
            [['\x91\xe5\xae\x89 '], '"\xe5\xae\x89 "\n'],
            # Good 2 bytes sequence.
            [['\xc3\x90'], '\xc3\x90\n'],
            # Bad third byte of 3 bytes sequence.
            [['\xe5\x8d'], '\n'],
            [['\xe5\x8d '], '" "\n'],
            # Good 3 bytes sequence.
            [['\xe5\x8d\x90'], '\xe5\x8d\x90\n'],
            # u+FFFF
            [['\xef\xbf\xbf'], '\n'],
            # Similar but not a surrogate
            [['\xed\x9f\xbf'], '\xed\x9f\xbf\n'],
            # Surrogate
            [['\xed\xa0\x80'], '\n'],
            # Surrogate
            [['\xed\xbf\xbf'], '\n'],
            # U+1F382 birthday cake
            [['\xf0\x9f\x8e\x82'], '\xf0\x9f\x8e\x82\n'],
            # Invalid first byte.
            [['\xff\xff\xff'], '\n'],
            # Incomplete utf-8
            [['\xe5\x87\xb8-\xe5\x87'], '\xe5\x87\xb8-\n'],
            [['\xe5\x87\xb8-\xe5'], '\xe5\x87\xb8-\n']]
        null_cases = [
            # Use \x5c instead of \\ to avoid python string escaping.
            [[None, None], ',\n']]
        utf16_cases = [
            # Bad second byte of 2 bytes sequence.
            [['\xc3'], '\n'],
            [['\xc3 '], '" "\n'],
            # Bad or missing second byte after valid utf-8.
            [['\xe5\xae\x89\x8d'], '\xe5\xae\x89\n'],
            [['\xe5\xae\x89\x8d '], '"\xe5\xae\x89 "\n'],
            # Bad or missing bytes intermixed in valid utf-8
            [['\x91\xea'], '\n'],
            [['\x90\xea\xae'], '\n'],
            [['\x90\x24\x90'], '\x24\n'],
            [['\x90 \x45\xae'], '" \x45"\n'],
            [['\x91 \x90 '], '"  "\n'],
            [['\xe5\x91 \x90 '], '"  "\n'],
            [['\x91\xe5\xae\x89 '], '"\xe5\xae\x89 "\n'],
            # Good 2 bytes sequence.
            [['\xc3\x90'], '\xc3\x90\n'],
            # Bad third byte of 3 bytes sequence.
            [['\xe5\x8d'], '\n'],
            [['\xe5\x8d '], '" "\n'],
            # Good 3 bytes sequence.
            [['\xe5\x8d\x90'], '\xe5\x8d\x90\n'],
            # u+FFFF
            [['\xef\xbf\xbf'], '\n'],
            # Similar but not a surrogate
            [['\xed\x9f\xbf'], '\xed\x9f\xbf\n'],
            # Surrogate
            [['\xed\xa0\x80'], '\n'],
            # Surrogate
            [['\xed\xbf\xbf'], '\n'],
            # U+1F382 birthday cake
            [['\xf0\x9f\x8e\x82'], '\n'],
            # Invalid first byte.
            [['\xff\xff\xff'], '\n'],
            # Incomplete utf-8
            [['\xe5\x87\xb8-\xe5\x87'], '\xe5\x87\xb8-\n'],
            [['\xe5\x87\xb8-\xe5'], '\xe5\x87\xb8-\n']]

        for case in escape_cases:
            result = CsvSerializer(False).serialize(case[0])
            self.verify(result, case)
        for case in utf8_cases:
            result = CsvSerializer(True).serialize(case[0])
            self.verify(result, case)
        for case in null_cases:
            result = CsvSerializer(True).serialize(case[0])
            self.verify(result, case)
        for case in utf16_cases:
            result = CsvSerializer(True, utf16_only=True).serialize(case[0])
            self.verify(result, case)

    def test_escaped_newline_filter(self):
        cases = [
            # Note 'a\x5c\nb\n' will lead to ['a\\\n', 'b\n'] without this filter.
            [['a\n', '\x5c\x5c\n', 'a\x5c\nb\n'], ['a\n', '\x5c\x5c\n', 'a\x5c\nb\n']],
            # Test end of a file without newline.
            [['a\n', 'a\x5c\nb'], ['a\n', 'a\x5c\nb']]]
        for case in cases:
            buf = StringIO()
            for l in case[0]:
                buf.write(l)
            buf.seek(0)
            with EscapedNewlineFilter(buf) as f:
                result = [l for l in f]
            self.verify(result, case)

if __name__ == '__main__':
    Test().test_csv_serializer()
    Test().test_escaped_newline_filter()
