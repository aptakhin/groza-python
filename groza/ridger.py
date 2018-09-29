from groza.postgres import PostgresDB


def parse(sub: str):
    ind = sub.rfind("_")
    table = sub[:ind]
    key = int(sub[ind + 1:])

    return table, key





class Ridger:
    def __init__(self, db: PostgresDB):
        self.db = db
        self.tables = {
            "boxes": ("box_id", {"tasks": ("box_id",),}),
            "tasks": ("task_id", {}),
        }

    async def start(self):
        pass

    async def fetch_sub(self, all_sub):
        resp = {}
        data = {}
        errors = []
        for sub, subDesc in all_sub.items():
            result = {}

            table, key = parse(sub)
            if table not in self.tables:
                errors.append(f"Table '{key}' is not handled")
                continue

            primary_key = self.tables[table][0]
            res = await self.db.fetchrow(f"SELECT * FROM {table} WHERE {primary_key} = $1", key)
            if not res:
                res = None
            else:
                res = dict(res)
                print(dict(res))

                fields = self.tables[table][1]

                sub_fields = subDesc.get("fields", [])
                if isinstance(sub_fields, str):
                    sub_fields = [sub_fields]

                for field in sub_fields:
                    if field not in fields:
                        errors.append(f"Invalid field '{field}' for type {sub}, have '%s'" % (', '.join(fields.keys()),))
                        continue

                    field_parent_id = fields[field][0]
                    res[field] = list(await self.db.fetch(f"SELECT * FROM {field} WHERE {field_parent_id} = $1", key))

            result["data"] = res
            result["status"] = "ok"

            data[sub] = result

        resp["data"] = data

        if errors:
            resp["errors"] = errors

        return resp
