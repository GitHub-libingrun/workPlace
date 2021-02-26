import re
import threading
import time
from queue import Queue
from lxml import etree
import requests
from utils import get_proxy, get_geo_hash, get_province_city, run_sql, p_c_dict, \
    get_province_city_for_add

detail_error_list=[]

headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36",
    }
def get_detail_html(url):
    for i in range(5):
        try:
            response = requests.get(url, headers=headers, proxies=get_proxy())
            if response.status_code == 200:
                response.encoding="gbk"
                print("当前运行url为-{}".format(url))
                return response.text
        except:
            pass


def parse_html(in_q):
    while in_q.empty() is not True:
        res = get_detail_html(url=in_q.get())
        if res:
            tree_html = etree.HTML(res)
            detail_url_list = tree_html.xpath("//ul[@class='dealers']/li/div[@class='detail']")
            detail_html = ""
            for detail in detail_url_list:
                try:
                    name = detail.xpath("./h2/a/text()")[0]
                    detail_url=detail.xpath("./h2/a/@href")[0]
                    shop_age =detail.xpath("./span[3]/text()")[0] if detail.xpath("./span[3]/text()") else ""
                    youhui="".join(detail.xpath("./p[1]//text()"))
                    if "优惠促销" in youhui:
                        group_purchase_info =youhui.split("促销 : ")[1] if len(youhui.split("促销 : "))==2 else ""
                    else:
                        group_purchase_info=""
                    for i in range(3):
                        try:
                            response = requests.get(detail_url, proxies=get_proxy())
                            response.encoding="gbk"
                            detail_html = response.text
                            break
                        except Exception as e:
                            print("代理有误" + repr(e))
                    detail_tree = etree.HTML(detail_html)
                    address = detail_tree.xpath("//div[@class='company-list']/a[@class='address']/text()")[0]
                    is_in = "0"
                    if address:
                        is_in_db = run_sql(
                            "SELECT COUNT(1) from tb_kache_data where source_url='%s';" % detail_url)
                        print(is_in_db[0][0])
                        if is_in_db[0][0] > 0:
                            print(address + "已存在")
                            is_in = "1"
                            # continue  # todo 如果此处第一次采集，不过滤，存在标记1，如果第二次补充采集，加上continue，地址存在就跳过此条
                    main_brand=detail_tree.xpath("//div[@class='company-list'][1]/span/text()")[0] if detail_tree.xpath("//div[@class='company-list'][1]/span/text()") else ""
                    tel = detail_tree.xpath("//div[@class='company-list']/span[@class='tel']/text()")[0] if detail_tree.xpath("//div[@class='company-list']/span[@class='tel']/text()") else ""
                    jwd=re.search("BMap.Point\((.*?)\);", detail_html, re.S)
                    jingweidu=jwd.group(1) if jwd else ""
                    if jingweidu:
                        lng =jingweidu.split(",")[0]
                        lat =jingweidu.split(",")[1]
                    else:
                        dict_s = get_province_city_for_add(address)
                        lng = dict_s["data"]["geocodes"][0]["location"].split(",")[0]
                        lat = dict_s["data"]["geocodes"][0]["location"].split(",")[1]
                    geo_hase = get_geo_hash(lng, lat)
                    # 获取经纬度，之后再通过经纬度解析地址
                    dict_s = get_province_city(lng, lat)
                    city_name = "" if dict_s["data"]["regeocode"]["addressComponent"]["city"]==[] else dict_s["data"]["regeocode"]["addressComponent"]["city"]
                    p_name = "" if dict_s["data"]["regeocode"]["addressComponent"]["province"] == [] else \
                        dict_s["data"]["regeocode"]["addressComponent"]["province"]
                    ad_name ="" if dict_s["data"]["regeocode"]["addressComponent"]["district"]==[] else dict_s["data"]["regeocode"]["addressComponent"]["district"]
                    if city_name=="" or city_name=="[]":
                        if p_name in "北京市上海市重庆市天津市":
                            city_name=ad_name
                        else:
                            city_name=""
                    pro_dict = p_c_dict.get(p_name, "")
                    p_code = ""
                    city_code = ""
                    ad_code = ""
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
                    source="卡车之家-全国卡车经销商"
                    source_url = detail_url
                    postcode = ""
                    if len(ad_name) > 0:
                        postcode = run_sql("select post_code from tb_cities where name='%s';" % ad_name)
                        if postcode:
                            postcode = postcode[0][0]
                    # ------------kong
                    print("insert into tb_kache_data(address,tel, postcode,city_name,city_code,p_name,ad_code,ad_name,lng,lat,name,geo_hase,p_code,shop_age,group_purchase_info,source,is_in,source_url,main_brand) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" % (
                        address, tel, postcode, city_name, city_code, p_name, ad_code, ad_name, lng, lat, name,
                        geo_hase, p_code,shop_age,group_purchase_info,
                        source,is_in,source_url,main_brand))
                    sqlstr = "insert into tb_kache_data(address,tel, postcode,city_name,city_code,p_name,ad_code,ad_name,lng,lat,name,geo_hase,p_code,shop_age,group_purchase_info,source,is_in,source_url,main_brand) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" % (
                        address, tel, postcode, city_name, city_code, p_name, ad_code, ad_name, lng, lat, name,
                        geo_hase, p_code,shop_age,group_purchase_info,
                        source,is_in,source_url,main_brand)
                    run_sql(sqlstr)
                except Exception as e:
                    print("nei--" + repr(e))
                    detail_error_list.append(detail_url)



if __name__ == "__main__":
    start = time.time()
    queue = Queue()
    # result_queue = Queue()
    for i in range(170, 881):
        queue.put("https://dealer.360che.com/dealer_1_0_0_0_0_0_c{}.html".format(i))
    print('queue 开始大小 %d' % queue.qsize())

    for index in range(10):
        thread = threading.Thread(target=parse_html, args=(queue,))
        thread.daemon = True  # 随主线程退出而退出
        thread.start()
    queue.join()  # 队列消费完 线程结束
    end = time.time()
    print('总耗时：%s' % (end - start))
    print('queue 结束大小 %d' % queue.qsize())
    # print('result_queue 结束大小 %d' % result_queue.qsize())
    # if len(detail_error_list) > 20:
    #     with open("error1.txt", "a", encoding="utf-8") as f:
    #         f.write(str(detail_error_list))