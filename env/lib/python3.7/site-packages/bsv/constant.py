# -*- coding: utf-8 -*-

## 品种代码对照表
# SHFE
SHFE_C2P = {'cu': '铜', 'al': '铝', 'zn': '锌', 'pb': '铅', 'sn': '锡', 'ni': '镍',
            'au': '黄金', 'ag': '白银',
            'rb': '螺纹钢', 'wr': '线材', 'hc': '热轧卷板', 'ss': '不锈钢',
            'fu': '燃料油', 'bu': '石油沥青', 'ru': '天然橡胶', 'sp': '纸浆', 'sc': '原油', 'nr': '20号胶',
            'lu': '低硫燃料油', 'bc': '铜(BC)'}
# SHFE_P2C = dict([(v, k) for k, v in SHFE_C2P.items()])

# DCE
DCE_P2C = {'豆粕': 'm', '豆一': 'a', '豆油': 'y', '鸡蛋': 'jd', '玉米': 'c', '玉米淀粉': 'cs',
           '胶合板': 'bb', '纤维板': 'fb', '棕榈油': 'p', '豆二': 'b',
           '焦煤': 'jm', '焦炭': 'j', '聚丙烯': 'pp', '聚乙烯': 'l', '铁矿石': 'i', '聚氯乙烯': 'v',
           '乙二醇': 'eg', '苯乙烯': 'eb', '粳米': 'rr', '液化石油气': 'pg', '生猪': 'lh'}
DCE_C2P = dict([(v, k) for k, v in DCE_P2C.items()])

# CZCE
CZCE_C2P = {'cf': '棉花', 'fg': '玻璃', 'jr': '粳稻', 'lr': '晚籼稻', 'ma': '甲醇', 'oi': '菜籽油',
            'pm': '普麦', 'ri': '早籼稻', 'rm': '菜籽粕', 'rs': '油菜籽', 'sf': '硅铁', 'sm': '锰硅',
            'sr': '白糖', 'ta': 'PTA', 'wh': '强麦', 'zc': '动力煤',  'cy': '棉纱', 'ap': '苹果',
            'wt': '硬麦', 'cj': '红枣', 'ur': '尿素', 'pf': '短纤', 'sa': '纯碱', 'pk': '花生'}
CZCE_P2C = dict([(v, k) for k, v in CZCE_C2P.items()])
CZCE_cn = {'早籼': '早籼稻', '晚籼': '晚籼稻', '菜油': '菜籽油', '菜粕': '菜籽粕', '菜籽': '油菜籽'}
CZCE_BEFORE_CHANGE = {'早籼稻': 'er', '菜籽油': 'ro', '强麦': 'ws', '动力煤': 'tc', '甲醇': 'me'}
CZCE_BEFORE_CHANGE_C2P = dict([(v, k) for k, v in CZCE_BEFORE_CHANGE.items()])
CZCE_ALL_C2P = {**CZCE_BEFORE_CHANGE_C2P, **CZCE_C2P}

# CFFEX
CFFEX_C2P = {'if': '沪深300', 'ih': '上证50', 'ic': '中证500', 't': '10年期国债', 'tf': '5年期国债', 'ts': '2年期国债'}
# CFFEX_P2C = dict([(v, k) for k, v in CFFEX_C2P.items()])

## 交易所数据网址
SHFE_URL = "http://www.shfe.com.cn/data/dailydata/kx/pm%s.dat"
# 大商所先批量下载每个交易日的压缩包（包含所有合约的txt文件）
DCE_URL = "http://www.dce.com.cn/publicweb/quotesdata/exportMemberDealPosiQuotesBatchData.html"
DCE_URL_HTML = "http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html"
CZCE_URL = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/%s/%s/FutureDataHolding.htm"
CZCE_URL_before_20100825 = "http://www.czce.com.cn/cn/exchange/jyxx/pm/pm%s.html"
CZCE_URL_before_20151008 = "http://www.czce.com.cn/cn/exchange/%s/datatradeholding/%s.htm"
CFFEX_URL = "http://www.cffex.com.cn/sj/ccpm/%s/%s/%s.xml"

## 品种数据起始日(大商所暂未用到）
"""
DCE_INIT = {'a': '20030102', 'm': '20050310', 'y': '20061023', 'p': '20071029', 'c': '20060606',
            'cs': '20141219', 'jd': '20131108', 'fb': '20131206', 'bb': '20131206',
            'l': '20070731', 'v': '20090601', 'pp': '20140228',
            'j': '20110415', 'jm': '20130322', 'i': '20131018'}
"""
CFFEX_INIT = {'if': '20100416', 'ih': '20150416', 'ic': '20150416', 't': '20150320', 'tf': '20130916', 'ts': '20180817'}
# 每个交易所开始公布成交持仓数据的日期
CONTRACT_INIT = {"CFFEX": "20100415", "SHFE": "20020106", "CZCE": "20050428", "DCE": "20040104"}

## 大商所在以下日期的zip包无数据
DCE_ZIP_MISSING = ['20080828', '20111202', '20131127', '20131128',
                   '20131129', '20160104', '20160107', '20180409', '20180502']
