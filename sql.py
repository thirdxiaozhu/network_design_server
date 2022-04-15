import pymysql
from datetime import datetime


class Sql:
    def __init__(self):
        self.db = pymysql.connect(host='bj-cynosdbmysql-grp-o8f6p4fm.sql.tencentcdb.com',
                                  port=25007,
                                  user='root',
                                  password='zoujiaxv7891@',
                                  charset='utf8mb4',
                                  database='Network')
        self.cursor = self.db.cursor(cursor=pymysql.cursors.DictCursor)

        self.cursor.execute("SELECT VERSION()")
        data = self.cursor.fetchone()

        print("Database version : %s " % data)

    def addUser(self, dict):
        sql = "INSERT INTO Users(account, password, nickname) VALUES(%s,%s,%s);"
        try:
            self.cursor.execute(
                sql, (dict.get("id"), dict.get("password"), dict.get("nickname")))
            self.db.commit()
            return 1000
        except:
            # 如果发生错误则回滚
            self.db.rollback()
            return 1001

    def searchUser(self, dict):
        sql = "SELECT `account`, `nickname`, `signature`, `isonline`, `headscul` FROM `Users` WHERE `account` = %s AND `password` = %s;"
        try:
            self.cursor.execute(
                sql, (dict.get("account"), dict.get("password")))
            result = self.cursor.fetchone()
            #如果用户存在并且未登录
            if result and result.get("isonline") == 0:
                sql_1 = "UPDATE `Users` SET `isonline` = %s WHERE `account` = %s;"
                self.cursor.execute(sql_1, (1, dict.get("account")))
                self.db.commit()
                return result
            else:
                return None
        except Exception as e:
            # 如果发生错误则回滚
            print(e)
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
            self.cursor.execute(sql_1, (dict.get("target")))
            result_1 = self.cursor.fetchone()

            self.cursor.execute(sql_2, (dict.get("account"), dict.get(
                "target"), dict.get("target"), dict.get("account")))
            result_2 = self.cursor.fetchone()

            #如果用户存在并且Friends表中没有这俩人好友关系
            if result_1:
                if result_2 is None:
                    sql = "INSERT INTO Friends(account_1, account_2) VALUES(%s,%s);"
                    self.cursor.execute(
                        sql, (dict.get("account"), dict.get("target")))
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
        sql = """SELECT `account_1`, `nickname`,`signature`, `isonline` FROM `Friends`, `Users` WHERE `account_2` = %s AND `Friends`.`account_1` = `Users`.`account`
                   UNION
                   SELECT `account_2`, `nickname`,`signature`, `isonline` FROM `Friends`, `Users` WHERE `account_1` = %s AND `Friends`.`account_2` = `Users`.`account`;
                """
        try:
            self.cursor.execute(
                sql, (dict.get("account"), dict.get("account")))
            result = self.cursor.fetchall()
            if result:
                return {"friends": result}
            else:
                return None
        except:
            # 如果发生错误则回滚
            self.db.rollback()
            return 0

    def getMessageRecord(self, dict):
        sql = """SELECT `sender`, `recipient`, `message`, `time` FROM `SingalChats` WHERE `sender` = %s AND `recipient` = %s 
                 UNION
                 SELECT `sender`, `recipient`, `message`, `time` FROM `SingalChats` WHERE `recipient` = %s AND `sender` = %s
                ORDER BY `time`;
             """
        try:
            self.cursor.execute(sql, (dict.get("account"), dict.get(
                "target"), dict.get("account"), dict.get("target")))
            result = self.cursor.fetchall()
            if result:
                return {"messages": result}
            else:
                return None
        except Exception as e:
            print(e)
            # 如果发生错误则回滚
            self.db.rollback()
            return 0

    def sendMessage(self, dict):
        print(dict)
        sql = "INSERT INTO `SingalChats`(`sender`, `recipient`, `message`) VALUES(%s,%s,%s);"

        try:
            self.cursor.execute(
                sql, (dict.get("account"), dict.get("target"), dict.get("message")))
            self.db.commit()
            return 1000
        except Exception as e:
            print(e)
            # 如果发生错误则回滚
            self.db.rollback()
            return 1001

    def getNewMessage(self, thread_sql, dict, lastTime):
        sql = """SELECT `sender`, `recipient`, `message`, `time` FROM `SingalChats` WHERE `sender` = %s AND `recipient` = %s AND `time` > %s
                 UNION
                 SELECT `sender`, `recipient`, `message`, `time` FROM `SingalChats` WHERE `recipient` = %s AND `sender` = %s AND `time` > %s
                 ORDER BY `time`;
              """

        try:
            thread_sql.cursor.execute(sql, (dict.get("account"), dict.get(
                "target"), lastTime, dict.get("account"), dict.get("target"), lastTime))
            result = thread_sql.cursor.fetchall()
            if result:
                return {"messages": result}
            else:
                return None
        except Exception as e:
            print(e)
            # 如果发生错误则回滚
            thread_sql.db.rollback()
            return 0

    def Logout(self, dict):
        sql = "UPDATE `Users` SET `isonline` = %s WHERE `account` = %s;"
        try:
            self.cursor.execute(sql, (0, dict.get("account")))
            self.db.commit()
            return 1000
        except Exception as e:
            # 如果发生错误则回滚
            print(e)
            self.db.rollback()
            return 1001

    def updateHead(self, dict):
        sql = "UPDATE `Users` SET `headscul` = %s WHERE `account` = %s;"
        try:
            self.cursor.execute(sql, (dict.get("filepath"), dict.get("account")))
            self.db.commit()
            return 1000
        except Exception as e:
            # 如果发生错误则回滚
            print(e)
            self.db.rollback()
            return 1001


    def __del__(self):
        self.db.close()


if __name__ == "__main__":
    sql = Sql()
