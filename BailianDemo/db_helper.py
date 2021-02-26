import pymysql
from utils_new import *

DB_CONFIG = {
    'host': "bailian-aliyun-demo.bl-ai.com",
    'port': 3306,
    'user': "demo",
    'password': "L8CaGN@D",
    'db': 'db_british_petroleum_demo'
}
# DB_CONFIG = {
#     'host': '100.64.72.109', # 数据库地址，本机 ip 地址 127.0.0.1
#     'port': 3306, # 端口
#     'user': 'root',  # 数据库用户名
#     'password': 'root', # 数据库密码
#     'db': 'ceshi'
# }


class Mysql:
    """
    集成连接池
    """

    def __init__(self, stream=False, dict_=False, autoclose=True):  # MySQLdb.cursors.SSCursor 流式游标
        self.autoclose = autoclose
        if stream == True:
            self._connect = pymysql.connect(cursorclass=pymysql.cursors.SSCursor, **DB_CONFIG)
        if dict_ == True:
            self._connect = pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **DB_CONFIG)
        else:
            self._connect = pymysql.connect(**DB_CONFIG)

    def __enter__(self):

        self.cursor = self._connect.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):

        self._connect.commit()
        self.cursor.close()
        if self.autoclose:
            self._connect.close()



def linshi(address):
    dict_s = get_province_city_for_add(address)
    lng = dict_s["data"]["geocodes"][0]["location"].split(",")[0]
    lat = dict_s["data"]["geocodes"][0]["location"].split(",")[1]
    geo_hase = get_geo_hash(lng, lat)
    # 获取经纬度，之后再通过经纬度解析地址
    dict_s = get_province_city(lng, lat)
    p_name = "" if dict_s["data"]["regeocode"]["addressComponent"]["province"] == [] else \
        dict_s["data"]["regeocode"]["addressComponent"]["province"]
    city_name = "" if dict_s["data"]["regeocode"]["addressComponent"]["city"] == [] else \
        dict_s["data"]["regeocode"]["addressComponent"]["city"]
    ad_name = "" if dict_s["data"]["regeocode"]["addressComponent"]["district"] == [] else \
        dict_s["data"]["regeocode"]["addressComponent"]["district"]
    if city_name == "" or city_name == "[]":
        if p_name in "北京市上海市重庆市天津市":
            city_name = ad_name
        else:
            city_name = ""
    pro_dict = p_c_dict.get(p_name, "")
    p_code = ""
    city_code = ""
    ad_code = ""
    if pro_dict:
        p_code = pro_dict["adcode"]
        for c in pro_dict["districts"]:
            if c["name"] == city_name:
                city_code = c["adcode"]
                for d in c["districts"]:
                    if d["name"] == ad_name:
                        ad_code = d["adcode"]
                        break
    return [p_name,city_name,ad_name,p_code,city_code,ad_code,lng,lat,geo_hase]
if __name__ == '__main__':
    with Mysql() as cursor:
        sql ="SELECT * FROM `tb_kache_data` WHERE `ad_name` = '河南蒙古族自治县' OR `p_name` LIKE '%香港%' LIMIT 0,1000;"
        cursor.execute(sql)
        datas = cursor.fetchall()
        print(datas)
        for data in datas:
            list_s=linshi(data[2])
            list_s.append(data[0])
            # list_s=tuple(list_s)
            # print(list_s)
            print("update tb_kache_data set p_name='%s',city_name='%s',ad_name='%s',p_code='%s',city_code='%s',ad_code='%s',lng='%s',lat='%s',geo_hase='%s' where id=%d;"%(list_s[0],list_s[1],list_s[2],list_s[3],list_s[4],list_s[5],list_s[6],list_s[7],list_s[8],list_s[9]))
            run_sql("update tb_kache_data set p_name='%s',city_name='%s',ad_name='%s',p_code='%s',city_code='%s',ad_code='%s',lng='%s',lat='%s',geo_hase='%s' where id=%d;"%(list_s[0],list_s[1],list_s[2],list_s[3],list_s[4],list_s[5],list_s[6],list_s[7],list_s[8],list_s[9]))
# #         if datas:
#             print(datas[0][0])
# #         'insert into user (id, name) values (%s, %s)', ['1', 'Michael']
