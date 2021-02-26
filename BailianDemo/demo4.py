import re
from lxml import etree
import requests
from utils_new import get_proxy, get_geo_hash, get_province_city, run_sql, p_c_dict, \
    get_province_city_for_add,post_code_dicts


detail_error_list = []
headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36",
    }

# 获取所有品牌链接
def get_all_brand():
    # 获取所有的品牌,然后请求品牌url，获取所有地区的url，请求地区的url，获取列表页，获取条数，判断有几页，进行翻页
    url="https://www.chinatruck.org/dealer/b490_area_0"
    for i in range(3):
        try:
            res=requests.get(url,headers=headers,proxies=get_proxy()).text
        except:
            pass
    html=etree.HTML(res)
    brand_url_list=list(set(html.xpath("//li/div[@class='brand-jxs-tab']/span/a/@href")))
    return brand_url_list


# 通过品牌链接获取  省份和url的字典
def get_add_url_by_brand(brand_url):
    add_url_dict={}
    for i in range(5):
        try:
            res=requests.get(brand_url,headers=headers,proxies=get_proxy()).text
            break
        except:
            pass
    try:
        html = etree.HTML(res)
        addr_url_list =html.xpath("//li/dl[@class='tab-jxs']/dd")
        for addr_url in addr_url_list:
            province=addr_url.xpath("./a/text()")[0]
            url=addr_url.xpath("./a/@href")[0]
            add_url_dict[province]=url
    # except:
    #     pass
    finally:
        return add_url_dict

# 获取详情页res
def get_detail_html(url):
    for i in range(5):
        try:
            response = requests.get(url, headers=headers, proxies=get_proxy())
            res = response.text
            print("当前运行url为{}".format(url))
            if res:
                return res
        except Exception as e:
            print("当前运行url{}出错{}".format(url,repr(e)))


def parse_html(address):
    try:
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
        if ad_name=="":                 # 如果adname为空，取township
            ad_name = "" if dict_s["data"]["regeocode"]["addressComponent"]["township"] == [] else \
                dict_s["data"]["regeocode"]["addressComponent"]["township"]
        if city_name == "" or city_name == "[]":
            if p_name in "北京市上海市重庆市天津市":
                city_name = ad_name
            else:
                city_name = ad_name
                ad_name = "" if dict_s["data"]["regeocode"]["addressComponent"]["township"] == [] else \
                    dict_s["data"]["regeocode"]["addressComponent"]["township"]
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
                        city_code = ad_code
                        break
            else:
                for c in pro_dict["districts"]:
                    if c["name"] == city_name:
                        city_code = c["adcode"]
                        for d in c["districts"]:
                            if d["name"] == ad_name:
                                ad_code = d["adcode"]
                                break
        return [p_name, city_name, ad_name, p_code, city_code, ad_code, lng, lat, geo_hase]
    except:
        print(address)


def write_data(d):
    with open("error6.txt", "a", encoding="utf-8") as f:
        f.write(d + ";")

def main():
    source = "卡车之家-经销商"
    brand_url_list = get_all_brand()
    for brand in brand_url_list:
        print("当前品牌{}".format(brand))
        add_url_dict = get_add_url_by_brand(brand)
        for key, value in add_url_dict.items():
            # key：省份，value：对应链接
            for i in range(1,10):
                d_url=value+f"_c{i}.html"
                res = get_detail_html(d_url)  # 请求列表页,获取总条数，来判断页数
                if res:
                    res = res.replace("</br>", "|")
                else:
                    write_data(d_url)
                    continue
                html=etree.HTML(res)
                detail_list=html.xpath("//div[@class='jxs-lists']/ul/li")
                if len(detail_list)>0 and len(detail_list)<16:
                    # 解析页面，获取内容
                    for detail in detail_list:
                        try:
                            name=detail.xpath("./a/h2/text()")[0].strip() if detail.xpath("./a/h2/text()") else ""
                            main_brand=detail.xpath("./p[1]//text()")[1].strip() if len(detail.xpath("./p[1]//text()"))==2 else ""
                            address=detail.xpath("./p[@class='address']/span/text()")[0].replace(" ","") if detail.xpath("./p[@class='address']/span/text()") else key
                            is_in="0"
                            if address:
                                is_in_db = run_sql("SELECT COUNT(1) from tb_kache_data where name='%s' and address='%s';" % (name,address))
                                if is_in_db[0][0] > 0:
                                    is_in = "1"
                                    # continue  # todo 如果此处第一次采集，不过滤，存在标记1，如果第二次补充采集，加上continue，地址存在就跳过此条
                            tela=detail.xpath("./p/span[@class='tel']/text()")[0].strip() if detail.xpath("./p/span[@class='tel']/text()") else ""
                            province=key
                            tel = ",".join(list(set(re.findall("\d+-\d+|\d{11}|\d{7,8}|\d+-\d+-\d+", tela)))) if tela else ""
                            p_list=parse_html(address)
                            if p_list and province in p_list[0]:
                                postcode=post_code_dicts.get(p_list[4],"")
                                print(
                                    "insert into tb_kache_data(name,address,postcode,tel,source,source_url,p_name, city_name, ad_name, p_code, city_code, ad_code, lng, lat, geo_hase,main_brand) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" %
                                    (name,address,postcode,tel,source,d_url,p_list[0],p_list[1],p_list[2],p_list[3],p_list[4],p_list[5],p_list[6],p_list[7],p_list[8],main_brand))
                                sqlstr = "insert into tb_kache_data(name,address,postcode,tel,source,source_url,p_name, city_name, ad_name, p_code, city_code, ad_code, lng, lat, geo_hase,main_brand,is_in) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');"%\
                                         (name,address,postcode,tel,source,d_url,p_list[0],p_list[1],p_list[2],p_list[3],p_list[4],p_list[5],p_list[6],p_list[7],p_list[8],main_brand,is_in)
                                run_sql(sqlstr)
                            else:
                                print("province-{}和plist[0]-{}不相等".format(province, p_list[0]))
                                p_list = parse_html(key+address)
                                if p_list and province in p_list[0]:
                                    postcode = post_code_dicts.get(p_list[4], "")
                                    print(
                                        "insert into tb_kache_data(name,address,postcode,tel,source,source_url,p_name, city_name, ad_name, p_code, city_code, ad_code, lng, lat, geo_hase,main_brand) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" %
                                        (name, address, postcode, tel, source, d_url, p_list[0], p_list[1], p_list[2],
                                         p_list[3], p_list[4], p_list[5], p_list[6], p_list[7], p_list[8], main_brand))
                                    sqlstr = "insert into tb_kache_data(name,address,postcode,tel,source,source_url,p_name, city_name, ad_name, p_code, city_code, ad_code, lng, lat, geo_hase,main_brand,is_in) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" % \
                                             (name, address, postcode, tel, source, d_url, p_list[0], p_list[1], p_list[2],
                                              p_list[3], p_list[4], p_list[5], p_list[6], p_list[7], p_list[8], main_brand,
                                              is_in)
                                    run_sql(sqlstr)
                                else:
                                    write_data(d_url)
                        except:
                            pass
                    if len(detail_list)<15:
                        break
                else:
                    break


if __name__ == "__main__":
    main()
#     SELECT * FROM `tb_kache_data` WHERE id  IN (SELECT MIN(id) FROM tb_kache_data GROUP BY main_brand);



