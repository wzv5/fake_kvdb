#-*- coding:utf-8 -*-

"""
使用MySQL模拟新浪云的kvdb，以用于兼容其他环境
kvdb = KVClient(host='localhost', port=3306, user='root', passwd='123456', db='fake_kvdb', charset='utf8')

wang zheng
2015.05.28
"""

import MySQLdb
import cPickle as pickle

class fake_kvdb(object):
    

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(**kwargs)
        #self._create_table()
        return super(fake_kvdb, self).__init__()

    def __del__(self):
        self.disconnect_all()

    def _create_table(self):
        cur = self.conn.cursor()
        n = cur.execute("CREATE TABLE IF NOT EXISTS `fake_kvdb` (`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, `key` VARCHAR(200) NOT NULL, `value` TEXT NOT NULL, PRIMARY KEY (`id`), UNIQUE (`key`) ) ENGINE = MyISAM CHARACTER SET utf8 COLLATE utf8_general_ci;")
        cur.close()
        return n != 0

    def get(self, key):
        if not isinstance(key, str):
            raise TypeError("key must be string")
        value = None
        cur = self.conn.cursor()
        n = cur.execute("SELECT `value` FROM `fake_kvdb` WHERE `key` = %s;", (key,))
        if n:
            value = cur.fetchone()[0]
            value = pickle.loads(value.encode("utf-8"))
        cur.close()
        return value

    def set(self, key, value):
        if not isinstance(key, str):
            raise TypeError("key must be string")
        value = pickle.dumps(value)
        cur = self.conn.cursor()
        n = cur.execute("UPDATE `fake_kvdb` SET `value` = %s WHERE `key` = %s;", (value, key))
        cur.close()
        if n:
            return True
        cur = self.conn.cursor()
        n = cur.execute("INSERT INTO `fake_kvdb` (`key`, `value`) VALUES (%s, %s);", (key, value))
        cur.close()
        return n != 0

    def getkeys_by_prefix(self, prefix, limit=100, marker=None):
        if not isinstance(prefix, str):
            raise TypeError("prefix must be string")
        if marker and not isinstance(marker, str):
            raise TypeError("marker must be string")
        prefix += "%"
        markerid = -1
        ret = []
        if marker:
            cur = self.conn.cursor()
            n = cur.execute("SELECT `id` FROM `fake_kvdb` WHERE `key` = %s;", (marker,))
            if n:
                markerid = cur.fetchone()[0];
            cur.close()
        cur = self.conn.cursor()
        if markerid != -1:
            n = cur.execute("SELECT `key` FROM `fake_kvdb` WHERE `key` LIKE %s AND `id` > %s LIMIT %s;", (prefix, markerid, limit))
        else:
            n = cur.execute("SELECT `key` FROM `fake_kvdb` WHERE `key` LIKE %s LIMIT %s;", (prefix, limit))
        if n:
            ret = cur.fetchall()
            ret = [i[0].encode("utf-8") for i in ret]
        cur.close()
        return ret

    def delete(self, key):
        if not isinstance(key, str):
            raise TypeError("key must be string")
        cur = self.conn.cursor()
        n = cur.execute("DELETE FROM `fake_kvdb` WHERE `key` = %s;", (key,))
        cur.close()
        return n != 0

    def disconnect_all(self):
        self.conn.close()

KVClient = fake_kvdb