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

                fields = self.tables[table][1]

                sub_fields = subDesc.get("fields", [])
                if isinstance(sub_fields, str):
                    sub_fields = [sub_fields]

                for field in sub_fields:
                    if field not in fields:
                        errors.append(f"Invalid field '{field}' for type {sub}, have '%s'" % (', '.join(fields.keys()),))
                        continue

                    field_parent_id = fields[field][0]
                    children = await self.db.fetch(f"SELECT * FROM {field} WHERE {field_parent_id} = $1 ORDER BY ord", key)

                    res[field] = [dict(child) for child in children]

            result["data"] = res
            result["status"] = "ok"

            data[sub] = result

        resp["data"] = data

        if errors:
            resp["errors"] = errors

        return resp

    async def query_insert(self, query, insert):
        resp = {}
        table = query["table"]
        if table not in self.tables:
            return {"errors": [f"Table '{table}' is not handled"]}

        desc = self.tables[table]

        idx = 1
        args = ()
        fields = []
        values = []
        for key, value in insert.items():
            if key == desc[0]:
                continue
            fields.append(f"{key}")
            values.append(f"${idx}")
            args += (value,)
            idx += 1

        fields_str = ", ".join(fields)
        values_str = ", ".join(values)

        primary_key = desc[0]

        query = f"INSERT INTO {table} ({fields_str}) VALUES ({values_str}) RETURNING {primary_key}"
        result = await self.db.fetchrow(query, *args)
        if not result:
            return {"status": "error", "message": "No result"}

        return {"status": "ok", primary_key: result[primary_key]}

    async def query_update(self, query, update):
        resp = {}
        table = query["table"]
        if table not in self.tables:
            return {"errors": [f"Table '{table}' is not handled"]}

        desc = self.tables[table]

        idx = 1
        args = ()
        fields = []
        for key, value in update.items():
            fields.append(f"{key} = ${idx}")
            args += (value,)
            idx += 1
        value_fields = ", ".join(fields)

        primary_key_field = desc[0]

        args += (query[primary_key_field],)
        primary_key_idx = idx

        idx += 1
        query = f"UPDATE {table} SET {value_fields} WHERE {primary_key_field} = ${primary_key_idx}"
        await self.db.execute(query, *args)

        return {"status": "ok"}
