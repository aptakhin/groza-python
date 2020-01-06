#
# async def register(self, user, register):
#     method = register["method"]
#     if method == "password":
#         email = register.get("data", {}).get("email")
#         if not email:
#             raise ValueError("Email can't be empty")
#         password = register.get("data", {}).get("password")
#         if not password:
#             raise ValueError("Password can't be empty")
#
#         q = Q.INSERT("users").set(
#             lastUpdatedBy=-1,
#             timeCreated=Q.Unsafe("now()"),
#             timeUpdated=Q.Unsafe("now()"),
#         ).returning("userId")
#         user = await self.main_db.fetchrow(q)
#
#         passhash = hashit(password)
#         q = Q.INSERT("users_logins").set(
#             lastUpdatedBy=user["userId"],
#             userId=user["userId"],
#             timeCreated=Q.Unsafe("now()"),
#             type=method,
#             main=email,
#             secondary=passhash,
#         ).returning("*")
#         auth = await self.main_db.fetchrow(q)
#
#         valid_until = datetime.now() + timedelta(days=1)
#         token = "abdasdas"
#
#         device = register.get("device", {})
#         add_data = {
#             "device": device,
#         }
#
#         q = Q.INSERT("users_auths").set(
#             lastUpdatedBy=auth["userId"],
#             userLoginId=auth["userLoginId"],
#             userId=auth["userId"],
#             timeCreated=Q.Unsafe("now()"),
#             timeLastAccess=Q.Unsafe("now()"),
#             validUntil=valid_until,
#             token=token,
#             data=json.dumps(add_data),
#         )
#         await self.main_db.fetchrow(q)
#
#         return {"status": "ok", "token": token, "userId": auth["userId"], "type": "login"}
#
# async def login(self, user, login):
#     method = login["method"]
#     if method == "password":
#         email = login.get("data", {}).get("email")
#         if not email:
#             raise ValueError("Password can't be empty")
#         password = login.get("data", {}).get("password")
#         if not password:
#             raise ValueError("Password can't be empty")
#
#         passhash = hashit(password)
#         auth = await self.main_db.fetchrow("""
#             SELECT * from_ users_logins where type=$1 AND main=$2 AND secondary=$3
#         """, method, email, passhash)
#         if not auth:
#             return {"status": "error", "code": 2, "message": "Can't find email/password pair"}
#
#         valid_until = datetime.now() + timedelta(days=1)
#         token = "abdasdas"
#
#         device = login.get("device", {})
#         add_data = {
#             "device": device,
#         }
#         q = Q.INSERT("users_auths").set(
#             lastUpdatedBy=auth["userId"],
#             userLoginId=auth["userLoginId"],
#             userId=auth["userId"],
#             timeCreated=Q.Unsafe("now()"),
#             timeLastAccess=Q.Unsafe("now()"),
#             validUntil=valid_until,
#             token=token,
#             data=json.dumps(add_data),
#         )
#         await self.main_db.execute(q)
#
#         return {"status": "ok", "token": token, "userId": auth["userId"], "type": "login"}
#
#     return {"status": "error", "code": 1}
#
# async def auth(self, user, token):
#     auth = await self.main_db.fetchrow("""
#         SELECT "userId", "validUntil" from_ users_auths where token=$1
#     """, token)
#
#     if not auth:
#         return {"status": "error", "message": "Token expired", "code": 2}
#
#     if auth["validUntil"] < datetime.now():
#         return {"status": "error", "message": "Token expired", "code": 3}
#
#     await self.main_db.execute("""
#         UPDATE users_auths set "timeLastAccess"=now() where token=$1
#     """, token)
#
#     return {"status": "ok", "type": "auth", "userId": auth["userId"]}
