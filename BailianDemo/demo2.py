import re
import time
from queue import Queue
from threading import Thread
from lxml import etree
import requests
from utils import get_proxy, get_geo_hash, get_province_city_for_add,run_sql,get_province_city,p_c_dict

detail_error_list = []
requests.DEFAULT_RETRIES = 5
# s = requests.session()
# s.keep_alive = False

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36",
}
def get_detail_html(url):
    # url = "http://www.chinacar.com.cn/serv/list_0_0_0_0_{}.html".format(page)
    for i in range(5):
        try:
            response = requests.get(url, headers=headers, stream=True,proxies=get_proxy(),timeout=10)
            if response.status_code == 200:
                print("当前运行url为-{}".format(url))
                # print("ip=======", response.raw._connection.sock.getpeername())
                return response.text
        except:
            pass


def parse_html(in_q):
    while in_q.empty() is not True:
        res=get_detail_html(url=in_q.get())
        if res:
            tree_html = etree.HTML(res)
            detail_url_list = tree_html.xpath("//h3/a/@href")
            for d_url in detail_url_list:
                try:
                    detail_html=""
                    detail_url = "http://www.chinacar.com.cn" + d_url
                    for i in range(3):
                        try:
                            result = requests.get(detail_url,headers=headers,stream=True, proxies=get_proxy(),timeout=10)
                            # result.encoding="gbk"
                            detail_html=result.text
                            if detail_html:
                                break
                        except Exception as e:
                            print("代理有误:" + repr(e))
                    if not detail_html:
                        detail_error_list.append(detail_url)
                        continue
                    detail_tree = etree.HTML(detail_html)
                    name = detail_tree.xpath("//div[@class='libResultTd']/div[1]/text()")[0].split("：")[1].strip()
                    address = detail_tree.xpath("//div[@class='libResultTd']/div[2]/text()")[0].split("：")[1].strip()
                    flag="0"
                    if address:
                        is_in_db = run_sql("SELECT COUNT(1) from tb_kache_data where source_url='%s';" % detail_url)
                        print(is_in_db[0][0],"address-"+address)
                        if is_in_db[0][0]>0:
                            print(detail_url+"已存在")
                            flag="1"
                            continue  # todo 如果此处第一次采集，不过滤，存在标记1，如果第二次补充采集，加上continue，地址存在就跳过此条
                    postcode = detail_tree.xpath("substring-after(//div[@class='libResultTd']/div[3]/text(),'：')")
                    tel_all = detail_tree.xpath("substring-after(//div[@class='libResultTd']/div[5]/text(),'：')")
                    tel=",".join(list(set(re.findall("\d+-\d+|\d{11}|\d{7,8}", tel_all))))
                    # ---------------地址逆向解析--------------------
                    if address!="" and len(address)>3:
                        # print("adddd=",address)
                        dict_s = get_province_city_for_add(address)
                    else:
                        print("add",address,"name",name)
                        dict_s = get_province_city_for_add(name)
                    lng = dict_s["data"]["geocodes"][0]["location"].split(",")[0]
                    lat = dict_s["data"]["geocodes"][0]["location"].split(",")[1]
                    geo_hase = get_geo_hash(lng, lat)
                    # 获取经纬度，之后再通过经纬度解析地址
                    dict_s=get_province_city(lng,lat)
                    p_name = "" if dict_s["data"]["regeocode"]["addressComponent"]["province"]==[] else dict_s["data"]["regeocode"]["addressComponent"]["province"]
                    city_name = "" if dict_s["data"]["regeocode"]["addressComponent"]["city"] == [] else \
                        dict_s["data"]["regeocode"]["addressComponent"]["city"]
                    ad_name = "" if dict_s["data"]["regeocode"]["addressComponent"]["district"] == [] else \
                    dict_s["data"]["regeocode"]["addressComponent"]["district"]
                    if city_name=="" or city_name=="[]":
                        if p_name in "北京市上海市重庆市天津市":
                            city_name=ad_name
                        else:
                            city_name=""
                    pro_dict=p_c_dict.get(p_name,"")
                    p_code=""
                    city_code=""
                    ad_code=""
                    if pro_dict:
                        p_code = pro_dict["adcode"]
                        if p_name in "北京市上海市重庆市天津市":
                            for c in pro_dict["districts"]:
                                if c["name"] == ad_name:
                                    ad_code = c["adcode"]
                                    city_code=ad_code
                                    break
                        else:
                            for c in pro_dict["districts"]:
                                if c["name"] == city_name:
                                    city_code = c["adcode"]
                                    for d in c["districts"]:
                                        if d["name"] == ad_name:
                                            ad_code = d["adcode"]
                                            break
                    source = "商用车网-全国商用车售后服务站"
                    source_url=detail_url
                    if len(postcode)<3:
                        postcode=run_sql("select post_code from tb_cities where name='%s';"%ad_name)
                        if postcode:
                            postcode = postcode[0][0]
                        else:
                            postcode=""
                    print("insert into tb_kache_data(address,tel, postcode,city_name,city_code,p_name,ad_code,ad_name,lng,lat,name,geo_hase,p_code,source,is_in,source_url) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" %  (address, tel, postcode, city_name, city_code, p_name, ad_code, ad_name, lng, lat, name, geo_hase,p_code,
                    source,flag,source_url))
                    sqlstr = "insert into tb_kache_data(address,tel, postcode,city_name,city_code,p_name,ad_code,ad_name,lng,lat,name,geo_hase,p_code,source,is_in,source_url) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" % (
                    address, tel, postcode, city_name, city_code, p_name, ad_code, ad_name, lng, lat, name, geo_hase,p_code,
                    source,flag,source_url)
                    run_sql(sqlstr)
                except Exception as e:
                    print(detail_url,",nei--" + repr(e))
                    detail_error_list.append(detail_url)
    in_q.task_done()



if __name__ == "__main__":
    start = time.time()
    queue = Queue()
    result_queue = Queue()
    for i in range(1, 1087):
        queue.put("http://www.chinacar.com.cn/serv/list_0_0_0_0_{}.html".format(i))
    print('queue 开始大小 %d' % queue.qsize())

    for index in range(10):
        thread = Thread(target=parse_html, args=(queue,))
        thread.daemon = True  # 随主线程退出而退出
        thread.start()

    queue.join()  # 队列消费完 线程结束
    end = time.time()
    print('总耗时：%s' % (end - start))
    print('queue 结束大小 %d' % queue.qsize())
    print('result_queue 结束大小 %d' % result_queue.qsize())
    if len(detail_error_list) > 20:
        with open("error.txt", "a", encoding="utf-8") as f:
            f.write(str(detail_error_list))
