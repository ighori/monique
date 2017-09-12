
class Token(object):

    def __init__(self, lineno, orig_s, start, end):
        self.lineno = lineno
        self.orig_s = orig_s
        self.start = start
        self.end = end

    @property
    def value(self):
        return self.orig_s[self.start:self.end]

    def contains_idx(self, idx):
        return self.start <= idx < self.end

    def __unicode__(self):
        return u'T@%s(%s, %s, %r)' % (self.lineno, self.start, self.end, self.value)
    __repr__ = __unicode__
    def __cmp__(self, other):
        if not isinstance(other, Token):
            return -1
        if self.orig_s is not other.orig_s:
            return -1
        return cmp((self.start, self.end), (other.start, other.end))
    def __hash__(self):
        return hash((self.start, self.end))

AWAITING_MATCHES = {
    '(': ')',
    '{': '}',
    '[': ']',
    '"': '"',
    '\'': '\'',
}


def tokenize(lineno,
             s,
             f_isspace=lambda i, x: x.isspace(),
             handle_matches=True,
             f_force_clear_awaiting=lambda i, x: False):
    token_start = None
    i = 0
    len_s = len(s)
    awaiting = []
    while True:
        if i == len_s:
            if token_start is not None:
                yield Token(lineno, s, token_start, i)
            return
        if awaiting and f_force_clear_awaiting(i, s[i]):
            del awaiting[:]
        is_last_awaiting = awaiting and (i > 0) and (s[i] == awaiting[-1]) and (s[i-1] != '\\')
        if is_last_awaiting:
            awaiting.pop()

        is_space = f_isspace(i, s[i])

        can_yield = is_space
        if not awaiting and can_yield and token_start is not None:
            yield Token(lineno, s, token_start, i+1 if is_last_awaiting else i)
            token_start = None
        if not can_yield:
            if token_start is None:
                token_start = i
            if handle_matches and not is_last_awaiting:
                if i == 0 or s[i-1] != '\\':
                    match = AWAITING_MATCHES.get(s[i])
                    if match is not None:
                        awaiting.append(match)
        i += 1

