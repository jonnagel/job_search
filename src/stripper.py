from html.parser import HTMLParser
from io import StringIO

class MLStripper(HTMLParser):
    """
    Example: bic.data['data'].body.apply(MLStripper.strip_tags)
    Example: bic.data['data'].body.apply(MLStripper.parse_clean_body)
    """
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = StringIO()

    def handle_data(self, d):
        self.text.write(d)

    def get_data(self):
        return self.text.getvalue()

    def strip_tags(html):
        s = MLStripper()
        s.feed(html)
        str_ = s.get_data()
        str_ = str_.replace('\n', '')
        str_ = str_.replace('\r', '')
        return(str_)

    def parse_clean_body(s):
        def chars_only(s_):
            out = []
            for sl in s_.split():
                word = ''.join([c for c in list(sl) if c in list(ascii_lowercase)])
                # TODO: remove
                out.append(word)
            out = ' '.join(out)
            while (out.find('  ') > 0):
                out = out.replace('  ', ' ')
            return(out)
        return chars_only(s.lower())
