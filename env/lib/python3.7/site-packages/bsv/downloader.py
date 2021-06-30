from bs4 import BeautifulSoup
import chardet
import datetime as dt
import os
import pandas as pd
import re
import requests
import shutil
import time
import zipfile

from .constant import (SHFE_URL, SHFE_C2P,
                       CFFEX_INIT, CFFEX_URL, CFFEX_C2P,
                       CZCE_URL_before_20151008, CZCE_URL, CZCE_URL_before_20100825,
                       CZCE_ALL_C2P, CZCE_cn, CZCE_BEFORE_CHANGE, CZCE_P2C,
                       DCE_URL, DCE_C2P, DCE_URL_HTML)

__all__ = ['hp_shfe', 'hp_czce', 'hp_dce', 'hp_dce_post', 'hp_cffex']

# volume, buy_hold, sell_hold 标识(成交量、买持、卖持)
_symbol = ('v', 'b', 's')
# DCE批量下载txt文件内中文标识对应关系
_mapping_symbol = {"成交量": "v", "持买单量": "b", "持卖单量": "s"}
_mapping_symbol_re = dict([(v, k) for k, v in _mapping_symbol.items()])
# 识别contract字符串的正则
_contract_pattern = re.compile(r'[A-Za-z]{1,2}(\d){3,4}')


def get_next_date(date, timedelta=1, timeformat='%Y%m%d'):
    """
    get the next date by default with the format of string '%Y%m%d'
    """
    next_date = dt.datetime.strptime(date, timeformat) + dt.timedelta(timedelta)
    # return date with format YYYY-MM-DD to fit the wind API
    return next_date.strftime(timeformat)


def _code_pattern(text):
    # czce品种名称
    if 'pta' in text or 'PTA' in text:  # 若pta在行字符串中，并且TA出现在具体合约持仓排名中，则返回固定
        return 'TA', 'PTA'
    # 搜索品种code，若不在品种信息text中，则需要进一步处理，2012年7月16日当日开始包含code; 
    # 动力煤tc换成zc，甲醇me换成ma，品种信息text变量均包含code
    # 早籼稻er换成ri，菜籽油ro换成oi，强麦ws换成wh
    match = re.search(r'[A-Za-z]{1,2}', text)
    if match:
        code = match.group().lower()
        return code, CZCE_ALL_C2P[code]
    else:  # 日期小于20120716
        result = re.sub(r':|：', '', text).replace('品种', '').split()[0]
        product = CZCE_cn.get(result, result)  # 规范品种中文名称
        return CZCE_BEFORE_CHANGE.get(product, CZCE_P2C[product]), product


def _html_from_requests(url, encoding):
    """
    通过requests获取bs4格式文件
    """
    _requests = requests.get(url)
    _requests.encoding = encoding
    _requests = BeautifulSoup(_requests.text, 'lxml')
    return _requests


def _items_from_td(tr, *args, ex="CZCE"):
    """
    默认CZCE解析单个tr（行）args = [tday, prodcut, contract, code]
    """
    _td_three_groups = tr.select("td")
    rank = int(_td_three_groups[0].get_text().strip())

    for _no in range(1, 10, 3):
        member = _td_three_groups[_no].get_text().strip()
        if not re.sub(r'\W+', '', member):  # member只包含非单词字符或为空字符串
            continue
        value, change = (int(_td_three_groups[_no + i].get_text().strip().replace(',', '')) for i in range(1, 3))
        _s = _symbol[_no // 3]
        _holding_value = args[0], ex, args[3].upper(), args[1], args[2].upper(), _s, \
                         rank, member, value, change
        yield _holding_value


def _download_zipfile(oneday_zip_abspath, url, post):
    """
    下载zip文件
    """
    r = requests.get(url, params=post)
    with open(oneday_zip_abspath, "wb") as f:
        f.write(r.content)


def _un_zip(oneday_zip_abspath, oneday_dir_abspath):
    """
    解压zip并解析文本
    """
    try:
        zip_file = zipfile.ZipFile(oneday_zip_abspath)  # 打开zip_abspath这个安装包
    except zipfile.BadZipFile:
        os.remove(oneday_zip_abspath)
    else:
        zip_file.extractall(oneday_dir_abspath)  # 解压zip_abspath到dir_abspath
        zip_file.close()
        time.sleep(0.5)
        # 修改名称中乱码
        for txt_file in os.listdir(oneday_dir_abspath):
            name_been_changed = txt_file.encode("cp437").decode('GBK')
            os.rename(os.path.join(oneday_dir_abspath, txt_file), os.path.join(oneday_dir_abspath, name_been_changed))


def _detect_encoding(txt):
    with open(txt, 'rb') as f:
        data = f.read()
        return chardet.detect(data)['encoding'].lower()


def _readlines_txt(txt, encoding=None):
    _encoding = _detect_encoding(txt)
    if _encoding == 'utf-8':
        encoding = 'utf-8'
    with open(txt, 'r', encoding=encoding) as f:
        content = f.readlines()
        return content


def _txt_parser(txt, oneday_dir_abspath, ex="DCE"):
    """
    输入txt文件名，返回生成器
    """
    contract = txt.split('_')[1].lower()
    _tday_from_txtname = txt.split('_')[0]
    code = re.sub(r'\d+', '', contract)
    if code in DCE_C2P:
        product = DCE_C2P[code]
        _s = None
        content = _readlines_txt(os.path.join(oneday_dir_abspath, txt))
        for line in content:
            _line_split = line.strip().split()
            if not _line_split:
                continue
            rank = _line_split[0].strip()
            if rank == "名次":
                _s = _mapping_symbol[_line_split[2].strip()]
            elif re.match(r'\d+', rank):
                rank = int(rank)
                member = _line_split[1].strip()
                value = int(_line_split[2].strip().replace(',', ''))
                change = int(_line_split[3].strip().replace(',', ''))
                _holding_value = (_tday_from_txtname, ex, code, product, contract, _s,
                                  rank, member, value, change)
                yield _holding_value


def hp_shfe(tday):
    """
    上期所解析json格式的原始数据
    """
    _url = SHFE_URL % tday
    r = requests.get(_url)
    if r.status_code == 200:
        r = r.json()
        for row in r['o_cursor']:
            # 排名: 1
            rank = row['RANK']
            # 合约: cu1805
            contract = row['INSTRUMENTID'].strip().lower()
            # 如果是（某个合约汇总，999）或者（期货公司与非期货公司汇总，"all"），则忽略，
            # 后者在2002年刚开始时没有
            if rank == 999 or 'all' in contract or 'actv' in contract:
                continue
            # 品种代码： cu
            code = re.sub(r'\d+', '', contract)
            # 品种中文： 铜
            product = SHFE_C2P[code]
            for _no, _s in enumerate(_symbol, 1):
                # 期货公司会员简称
                member = row['PARTICIPANTABBR' + str(_no)].strip()
                # 若无会员信息，则忽略
                if not member:
                    continue
                # 成交量、买持、卖持
                value = row['CJ' + str(_no)]
                # 增减，绝对量，非百分比
                change = row['CJ' + str(_no) + '_CHG']
                _holding_value = (tday, "SHFE", code, product, contract, _s,
                                  rank, member, value, change)
                yield _holding_value


def hp_cffex(tday):
    """
    cffex的品种成交持仓排名在各自网页中，如果cffex中增加新品种，则需要在CFFEX_INIT更新
    """
    for code, _init_tday in CFFEX_INIT.items():
        if _init_tday > tday:  # 若品种上市日期在指定交易日之后，则忽略
            continue
        else:
            _url = CFFEX_URL % (tday[:-2], tday[-2:], code.upper())
            r = _html_from_requests(_url, 'utf-8')
            for data in r.select("data"):
                member = data.shortname.get_text().strip()
                if not member:  # 忽略空白行
                    continue
                product = CFFEX_C2P[code]
                contract = data.instrumentid.get_text().strip()
                _s = _symbol[int(data.datatypeid.get_text().strip())]
                rank = int(data.rank.get_text().strip())
                value = int(data.volume.get_text().strip())
                change = int(data.varvolume.get_text().strip())
                _holding_value = (tday, "CFFEX", code, product, contract, _s,
                                  rank, member, value, change)
                yield _holding_value


def hp_czce(tday):
    """
    郑商所在2010年8月25日切换至新的页面
    """
    if tday >= '20100825':
        if tday < '20151008':
            _url = CZCE_URL_before_20151008 % (tday[:4], tday)
            _css_string = "#toexcel table tr tr"
            r = _html_from_requests(_url, 'utf-8')
        else:
            _url = CZCE_URL % (tday[:4], tday)
            _css_string = "tr"
            r = _html_from_requests(_url, 'utf-8').select('table')
            if len(r):
                r = r[-1]
            else:
                return
        table = r.select(_css_string)
        # 品种：合约汇总 | 合约：单个合约
        for i, tr in enumerate(table):
            text = tr.td.get_text()
            if "品种" in text:
                code, product = _code_pattern(text)
                contract = 'all'
            elif "合约" in text:
                contract = _contract_pattern.search(text).group().lower()
                code = re.sub(r'\d+', '', contract)
                product = CZCE_ALL_C2P[code]
            elif '名次' in text or '合计' in text:
                continue
            else:
                # tr包含10个td，1+9
                yield from _items_from_td(tr, tday, product, contract, code)
    # # 郑商所2010年8月25日之前的交易日
    else:
        _url = CZCE_URL_before_20100825 % tday
        r = _html_from_requests(_url, 'utf-8')
        for _div, _table in zip(r.select('table > div'), r.select('table > table')):
            text = _div.get_text()
            if "品种" in text:
                try:
                    code, product = _code_pattern(text)
                except KeyError:
                    continue
                contract = 'all'
            elif "合约" in text:
                contract = _contract_pattern.search(text).group().lower()
                code = re.sub(r'\d+', '', contract)
                product = CZCE_ALL_C2P[code]
            # 在有合约或者品种的情况下，读取table内容
            for tr in _table.select("tr")[:-1]:
                # tr包含10个td，1+9
                if '名次' in tr.get_text() or '合计' in tr.get_text():
                    continue
                yield from _items_from_td(tr, tday, product, contract, code)


def hp_dce(tday, oneday_dir_abspath, oneday_zip_abspath):
    # 月份0-11
    post = {"year": tday[:4], "month": str(int(tday[4:6]) - 1), "day": tday[6:]}
    # 解压缩文件保存路径
    oneday_dir_abspath = os.path.join(oneday_dir_abspath, "dce_batch_%s" % tday)
    if os.path.exists(oneday_dir_abspath):
        shutil.rmtree(oneday_dir_abspath)  # 如存在解压后的目标文件夹路径，删除文件夹
    # 压缩包保存路径
    oneday_zip_abspath = os.path.join(oneday_zip_abspath, "dce_batch_%s.zip" % tday)
    # 下载压缩包
    _download_zipfile(oneday_zip_abspath, DCE_URL, post)
    time.sleep(1.0)
    # 解压缩文件
    _un_zip(oneday_zip_abspath, oneday_dir_abspath)
    if os.path.exists(oneday_dir_abspath):
        for txt_file in os.listdir(oneday_dir_abspath):
            yield from _txt_parser(txt_file, oneday_dir_abspath)


def hp_dce_post(tday, code, contract, product):
    """
    通过提交get参数获取返回响应，解析相关数据，并非通过下载压缩包
    """
    time.sleep(0.5)
    post = {"year": tday[:4], "month": str(int(tday[4:6]) - 1), "day": tday[6:],
            "contract.contract_id": contract.lower(), "contract.variety_id": code.lower(),
            "memberDealPosiQuotes.variety": code.lower()}
    r = requests.get(DCE_URL_HTML, params=post)
    tables = pd.read_html(r.text, header=0)
    if len(tables) and not tables[-1].empty:
        temp = tables[-1]
        for _column_no, _s in enumerate(_symbol):
            for _row in temp.iloc[:, (_column_no * 4):(_column_no + 1) * 4].values:
                rank = _row[0]
                if rank == '总计' or pd.isnull(rank):
                    continue
                member = _row[1].strip()
                value = int(_row[2])
                change = int(_row[3])
                _holding_value = (tday, 'DCE', code.upper(), product, contract.upper(), _s,
                                  int(rank), member, value, change)
                yield _holding_value


if __name__ == '__main__':
    for row in hp_shfe('20190701'):
        print(row)
