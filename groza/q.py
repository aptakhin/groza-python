
def quoted(s):
    return '"%s"' % s


class Expr:
    Arg = "arg"
    Kwarg = "kwarg"

    def __init__(self, tp, key, value=None):
        self.tp = tp
        self.key = key
        self.value = value

    def format(self, idx):
        print("F", idx, self.tp, self.key, self.value)
        # insert = quoted(self.key) if self.key is not None else None
        # key = "$%d" % idx if self.key is not None else self.value
        # value = (self.value,) if self.key is not None else ()

        if self.tp == self.Arg:
            # if self.key is None and insert:
            #     raise ValueError("Can't handle Arg and insert=True together")
            if self.key and "{}" in self.key:
                insert = None
                key = self.key.replace("{}", "$%d" % idx)
            else:
                insert = quoted(self.key) if self.key is not None else None
                key = "$%d" % idx if self.key is not None else self.value

            value = (self.value,) if self.key is not None else ()

            res = insert, key, value
        elif self.tp == self.Kwarg:
            insert = quoted(self.key) if self.key is not None else None
            key = "$%d" % idx if self.key is not None else self.value
            value = (self.value,) if self.key is not None else ()

            res = insert, key, value

        print(res)
        return res


class Q:
    _SELECT = "SELECT"

    @staticmethod
    def SELECT(fields=None):


        q = Q(main_cmd=Q._SELECT)
        if fields is None:
            fields = "*"
        elif isinstance(fields, str):
            fields = fields.split(",")
        q.select_fields = fields
        return q

    def __init__(self, main_cmd=None):
        self.main_cmd = main_cmd
        assert main_cmd in (None, Q._SELECT,)
        self._FROM = None
        self.select_fields = None
        self.update_fields = None
        self._WHERE = []

    def FROM(self, table):
        self._FROM = table
        return self

    def WHERE(self, *args, **kwargs):
        if args and kwargs:
            raise RuntimeError("Can't understand args and kwargs in WHERE")
        if args:
            key = args[0] if len(args) > 1 else None
            value = args[1] if len(args) > 1 else args[0]
            self._WHERE.append(Expr(Expr.Arg, key, value))
        for key, value in kwargs.items():
            self._WHERE.append(Expr(Expr.Kwarg, key, value))
        return self

    def END(self, debug_print=True):
        q = ""
        q_args = ()

        q += self.main_cmd

        if self.select_fields not in (None, "*"):
            q += " " + ",".join(self.select_fields)
        else:
            q += " *"

        q += " FROM " + self._FROM

        idx = 1
        if self._WHERE:
            q += " WHERE"
            where_fields = []
            where_args = ()
            for wh in self._WHERE:
                add_insert, add_value, add_args = wh.format(idx)
                if add_insert:
                    where_fields.append(f"{add_insert}={add_value}")
                else:
                    where_fields.append(f"{add_value}")
                where_args += add_args
            q += " " + ",".join(where_fields)
            q_args += where_args

        if debug_print:
            print('Q: %s; %s' % (q, q_args))

        return q, q_args


if __name__ == "__main__":
    assert Expr(Expr.Arg, "a", 5).format(1) == ('"a"', "$1", (5,))
    assert Expr(Expr.Arg, None, 'a!=5').format(1) == (None, 'a!=5', ())
    assert Expr(Expr.Arg, "a=ANY({})", [2, 4]).format(1) == (None, 'a=ANY($1)', ([2, 4],))
    assert Expr(Expr.Arg, "a<={}", 3).format(1) == (None, 'a<=$1', (3,))
    assert Expr(Expr.Kwarg, "a", 5).format(1) == ('"a"', "$1", (5,))

    assert Q.SELECT().FROM("foo").WHERE(a=5).END() == ('SELECT * FROM foo WHERE "a"=$1', (5,))
    assert Q.SELECT().FROM("foo").WHERE("a!=5").END() == ('SELECT * FROM foo WHERE a!=5', ())
    assert Q.SELECT().FROM("foo").WHERE("a=ANY({})", [2, 4]).END() == ('SELECT * FROM foo WHERE a=ANY($1)', ([2, 4],))
    assert Q.SELECT().FROM("foo").WHERE("a<={}", 3).END() == ('SELECT * FROM foo WHERE a<=$1', (3,))
    assert Q.SELECT("boo").FROM("foo").WHERE("a=5").END() == ('SELECT boo FROM foo WHERE a=5', ())
