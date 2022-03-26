import pymysql

class Sql:
    def __init__(self):
        self.db = pymysql.connect(host='bj-cynosdbmysql-grp-o8f6p4fm.sql.tencentcdb.com',
                    port = 25007,
                    user='root',
                    password='zoujiaxv7891@',
                    database='Network')
        self.cursor = self.db.cursor(cursor=pymysql.cursors.DictCursor)

        self.cursor.execute("SELECT VERSION()")
        data = self.cursor.fetchone()

        print ("Database version : %s " % data)


    def addUser(self, dict):
        sql = "INSERT INTO Users(account, password, nickname) VALUES(%s,%s,%s);"
        try:
            self.cursor.execute(sql,(dict.get("id"), dict.get("password"), dict.get("nickname")))
            self.db.commit()
            return 1000
        except:
            # 如果发生错误则回滚
            self.db.rollback()
            return 1001

    def searchUser(self, dict):
        sql_1 = "SELECT `account`, `nickname`, `signature` FROM `Users` WHERE `account` = %s AND `password` = %s;"
        try:
            self.cursor.execute(sql_1,(dict.get("account"), dict.get("password")))
            result_1 = self.cursor.fetchone()
            print(result_1)
            if result_1:
                return result_1
            else:
                return None
        except:
            # 如果发生错误则回滚
            self.db.rollback()
            return 0

    def addFriend(self, dict):
        sql_1 = "SELECT `account` FROM `Users` WHERE `account` = %s;"
        sql_2 = """
                    SELECT `account_1`, `account_2` FROM `Friends` WHERE `account_1` = %s AND `account_2` = %s
                    UNION
                    SELECT `account_1`, `account_2` FROM `Friends` WHERE `account_1` = %s AND `account_2` = %s;
                """
        try:
            self.cursor.execute(sql_1,(dict.get("target")))
            result_1 = self.cursor.fetchone()

            self.cursor.execute(sql_2,(dict.get("account"), dict.get("target"), dict.get("target"), dict.get("account")))
            result_2 = self.cursor.fetchone()

            #如果用户存在并且Friends表中没有这俩人好友关系
            if result_1:
                if result_2 is None:
                    sql = "INSERT INTO Friends(account_1, account_2) VALUES(%s,%s);"
                    self.cursor.execute(sql,(dict.get("account"), dict.get("target")))
                    self.db.commit()
                    return 1000
                else:
                    return 1002
            else:
                return 1001
        except:
            # 如果发生错误则回滚
            self.db.rollback()
            return 0

    def searchFriend(self, dict):
        sql_2 = """SELECT `account_1`, `nickname`,`signature` FROM `Friends`, `Users` WHERE `account_2` = %s AND `Friends`.`account_1` = `Users`.`account`
                   UNION
                   SELECT `account_2`, `nickname`,`signature` FROM `Friends`, `Users` WHERE `account_1` = %s AND `Friends`.`account_2` = `Users`.`account`;
                """
        try:
            self.cursor.execute(sql_2,(dict.get("account"), dict.get("account")))
            result = self.cursor.fetchall()
            if result:
                return {"friends" : result}
            else:
                return None
        except:
            # 如果发生错误则回滚
            self.db.rollback()
            return 0

        



    def __del__(self):
        self.db.close()


if __name__=="__main__":
    sql = Sql()