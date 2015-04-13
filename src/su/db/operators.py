
class BooleanOp(object):
    def __init__(self, *ops):
        self.ops = ops

    def __repr__(self):
        return '<%s_ %s>' % (self.__class__.__name__, str(self.ops))

class or_(BooleanOp): pass
class and_(BooleanOp): pass
class not_(BooleanOp): pass

class op(object):
    def __init__(self, lval, lval_name, rval):
        self.lval = lval
        self.rval = rval
        self.lval_name = lval_name

    def __repr__(self):
        return '<%s: %s, %s>' % (self.__class__.__name__, self.lval, self.rval)

    #sorts in a consistent order, required for Query._iden()
    def __lt__(self, other):
        return repr(self) < repr(other)

class eq(op): pass
class ne(op): pass
class lt(op): pass
class lte(op): pass
class gt(op): pass
class gte(op): pass
class in_(op): pass


class Slot(object):
    def __init__(self, lval):
        if isinstance(lval, Slot):
            self.name = lval.name
            self.lval = lval
        else:
            self.name = lval

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.name)

    def __eq__(self, other):
        return eq(self, self.name, other)

    def __ne__(self, other):
        return ne(self, self.name, other)

    def __lt__(self, other):
        return lt(self, self.name, other)

    def __le__(self, other):
        return lte(self, self.name, other)

    def __gt__(self, other):
        return gt(self, self.name, other)

    def __ge__(self, other):
        return gte(self, self.name, other)

    def in_(self, other):
        return in_(self, self.name, other)

class Slots(object):
    def __getattr__(self, attr):
        return Slot(attr)

    def __getitem__(self, attr):
        return Slot(attr)
        
def op_iter(ops):
    for o in ops:
        if isinstance(o, op):
            yield o
        elif isinstance(o, BooleanOp):
            for p in op_iter(o.ops):
                yield p

class query_func(Slot): pass
class lower(query_func): pass
class ip_network(query_func): pass
class base_url(query_func): pass
class domain(query_func): pass
class year_func(query_func): pass

class timeago(object):
    def __init__(self, interval):
        self.interval = interval

    def __repr__(self):
        return '<interval: %s>' % self.interval

class sort(object):
    def __init__(self, col):
        self.col = col

    def __repr__(self):
        return '<sort:%s %s>' % (self.__class__.__name__, str(self.col))

    def __eq__(self, other):
        return self.col == other.col

class asc(sort): pass
class desc(sort):pass
class shuffled(desc): pass
