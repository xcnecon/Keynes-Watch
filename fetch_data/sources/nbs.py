"""
NBSFetcher -- fetcher for National Bureau of Statistics data.

Series:
    nbs_real_estate_climate    国房景气指数 (via AKShare)
    nbs_house_price            70城住宅价格指数 — 汇总 + 分面积 (via NBS 新版数据发布库 API「主要城市月度价格」+ 官方新闻稿回退)
    nbs_real_estate_macro      全国房地产月度宏观数据 — 9大类 (via NBS easyquery API, dbcode=hgyd)
    cn_flow_of_funds           资金流量表（非金融交易）年度 1992+ (via NBS 新版数据发布库 API)
    nbs_retail_sales           社会消费品零售总额月度 1984+ (via NBS 新版数据发布库 API + 官方新闻稿回退)

Requires: pip install akshare lxml
"""

import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from io import StringIO
from urllib.parse import urljoin

import akshare as ak
import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from fetch_data.base import BaseFetcher, AbortFetch


class NBSThrottledError(AbortFetch):
    """NBS v2 WAF 熔断已跳闸：本轮剩余 v2 请求直接放弃，等下次运行自愈。

    网宿 WAF 按 IP 限流，触发后返回空 body 或直接掐断连接，且短时间内不会
    恢复——继续逐城/逐叶重试只会延长封禁（2026-07-06 实测）。
    """


# ---------------------------------------------------------------------------
# 70城住宅销售价格指数（NBS 新版数据发布库「主要城市月度价格」，2026-07 迁移）
# 旧 easyquery csyd 通道已 403。树根 code=7；叶子 = 70个大中城市住宅销售价格指数。
# 取数按城市逐个请求（das = 12 位行政区划码）。
# ⚠ 服务端对无效/多城市 das 会【静默回退返回北京】——2026-03 初次回填时
#   由此把北京 180 个月历史写进了 15 个城市（含不在 70 城调查内的拉萨）。
#   解析时必须校验响应 da_name 与请求城市一致，绝不能只信请求参数。
# ---------------------------------------------------------------------------
NBS_V2_CITYPRICE_ROOT = '327ecbb2e6b14c669da1e99e39faa24c'  # 主要城市月度价格根目录
HP_V2_CID = '3eb43764c74741469b745c396cf002d1'  # 70个大中城市住宅销售价格指数叶子

# 表字段 -> 指标 UUID（queryIndicatorsByCid 枚举，2026-07 实测；
# new=新建商品住宅、used=二手住宅；上月=100 → mom、上年同月=100 → yoy）
HP_V2_INDICATORS = {
    'new_mom':           '732f9cca00c84facb9bb8dd8365bc0e7',
    'new_yoy':           'fb43046325f64e3896a96b70b071d52b',
    'used_mom':          '05dd255eb4d54986a567d523b4403676',
    'used_yoy':          '11ad09962ea7497eb2ccdf2be5719a20',
    'new_90below_mom':   '25205b9fb3054cc89ff1d1e0fc4bc110',
    'new_90below_yoy':   '2624e01a782d4f47a23610bd056570ae',
    'new_90to144_mom':   '73a8b0fe738a482491ed17e9b28e0d85',
    'new_90to144_yoy':   'cbcf12312fec43768ead1af77fd26ed0',
    'new_144above_mom':  '93815f78b2d54fc98a34b9669be5467f',
    'new_144above_yoy':  '757f2c9afc39420d89c47c30890d9de5',
    'used_90below_mom':  '98fb96672e1c4666adb821e393499b46',
    'used_90below_yoy':  'e9a41436e75841bc95eaadb21ab1f02e',
    'used_90to144_mom':  '45871773323240bb91dddbfa5e2a1b55',
    'used_90to144_yoy':  '0a73708bfa9e45798883a9b186b7f703',
    'used_144above_mom': '3b970e6f7834459b909a2a9203242c2e',
    'used_144above_yoy': 'dae265212c9f4e48bbbd9038362f628e',
}

HP_COL_ORDER = list(HP_V2_INDICATORS)  # 与 SERIES['columns'][2:] 顺序一致

# 官方 70 个大中城市（短名 = 数据库 city 值，6 位行政区划码；拉萨不在调查范围）。
# 2026-07 已逐城实测：v2 库覆盖 2011-01 起，da_name 回显与短名前缀匹配
# （大理 → 大理白族自治州）。
HP_CITIES = [
    ('北京', '110000'), ('天津', '120000'), ('石家庄', '130100'), ('唐山', '130200'),
    ('秦皇岛', '130300'), ('太原', '140100'), ('呼和浩特', '150100'), ('包头', '150200'),
    ('沈阳', '210100'), ('大连', '210200'), ('丹东', '210600'), ('锦州', '210700'),
    ('长春', '220100'), ('吉林', '220200'), ('哈尔滨', '230100'), ('牡丹江', '231000'),
    ('上海', '310000'), ('南京', '320100'), ('无锡', '320200'), ('徐州', '320300'),
    ('扬州', '321000'), ('杭州', '330100'), ('宁波', '330200'), ('温州', '330300'),
    ('金华', '330700'), ('合肥', '340100'), ('蚌埠', '340300'), ('安庆', '340800'),
    ('福州', '350100'), ('厦门', '350200'), ('泉州', '350500'), ('南昌', '360100'),
    ('九江', '360400'), ('赣州', '360700'), ('济南', '370100'), ('青岛', '370200'),
    ('烟台', '370600'), ('济宁', '370800'), ('郑州', '410100'), ('洛阳', '410300'),
    ('平顶山', '410400'), ('武汉', '420100'), ('宜昌', '420500'), ('襄阳', '420600'),
    ('长沙', '430100'), ('岳阳', '430600'), ('常德', '430700'), ('广州', '440100'),
    ('韶关', '440200'), ('深圳', '440300'), ('湛江', '440800'), ('惠州', '441300'),
    ('南宁', '450100'), ('桂林', '450300'), ('北海', '450500'), ('海口', '460100'),
    ('三亚', '460200'), ('重庆', '500000'), ('成都', '510100'), ('泸州', '510500'),
    ('南充', '511300'), ('贵阳', '520100'), ('遵义', '520300'), ('昆明', '530100'),
    ('大理', '532900'), ('西安', '610100'), ('兰州', '620100'), ('西宁', '630100'),
    ('银川', '640100'), ('乌鲁木齐', '650100'),
]

NBS_API_URL = 'https://data.stats.gov.cn/easyquery.htm'
NBS_RELEASE_LIST_URL = 'https://www.stats.gov.cn/sj/zxfb/'
NBS_RELEASE_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/131.0.0.0 Safari/537.36'),
}

# ---------------------------------------------------------------------------
# NBS easyquery API — 全国房地产月度宏观指标分类 (dbcode=hgyd)
# Parent: A06 = "房地产"
# ---------------------------------------------------------------------------
NBS_MACRO_CATEGORIES = {
    'A0601': '房地产开发投资情况',
    'A0602': '房地产开发投资实际到位资金',
    'A0603': '房地产土地开发与销售情况',
    'A0604': '房地产施工、竣工面积',
    'A0605': '商品住宅施工、竣工面积',
    'A0608': '商品房销售面积',
    'A0609': '商品房销售额',
    'A060A': '商品住宅销售面积',
    'A060B': '商品住宅销售额',
}

# ---------------------------------------------------------------------------
# NBS 新版数据发布库 API（2026 年 6 月上线，UUID 制，旧 easyquery 已 403）
# 协议: GET  {BASE}/new/queryIndexTreeAsync?pid=<uuid>&code=3   目录树（年度=code 3）
#       GET  {BASE}/new/queryIndicatorsByCid?cid=<uuid>&dt=&name=  叶子目录下指标元数据
#       POST {BASE}/stream/esData  {cid, indicatorIds, das, dts, rootId, ...}  批量取值
# 资金流量表（非金融交易）年度序列 1992 起，路径：
#   年度数据 → 国民经济核算 → 资金流量表（非金融交易）→ 各机构部门 → 资金运用/来源
# ---------------------------------------------------------------------------
NBS_V2_BASE = 'https://data.stats.gov.cn/dg/website/publicrelease/web/external'
NBS_V2_ANNUAL_ROOT = '884c062607104a91967b22742537f44f'  # 年度数据根目录 UUID
NBS_V2_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/131.0.0.0 Safari/537.36'),
    'Referer': 'https://data.stats.gov.cn/dg/website/page.html',
}

# (db variable, leaf catalog cid, indicator uuid)
# 单位均为亿元。cid 为「部门|运用/来源」叶子目录，indicator 为其中具体行。
FOF_INDICATORS = [
    # ---- 各部门总储蓄（资金来源） ----
    ('hh_saving',    '18bf1d34b2aa43b3be8d0814fbefca29', '369841a2a762400f9140ab5c7a8de81e'),  # 住户
    ('gov_saving',   'fadaf98df4624baaafbb354ada72ed06', '70eab9f2938c4da0a7f42d2361917f1a'),  # 广义政府
    ('nf_saving',    'c8cb66f46d314a039e747f426509dbd7', 'e87cf86319f046a085ba70293f218b34'),  # 非金融企业
    ('fin_saving',   'ff99935ac60a4ebe89cda3bd53349ab9', '82eb62633c6d43b08b5d8d619963146a'),  # 金融机构
    ('row_saving',   'baf3bf0ae7f94b7384babc624c862a95', '5506622d910549d38ebd4b5e8f6ed211'),  # 国外(=-经常账户)
    # ---- 各部门净金融投资（资金运用，即部门净贷出） ----
    ('hh_nfi',       'b0e282fb1220433197b41d5f95d38972', '4adb758a259541e6aaa09754acb004bd'),  # 住户
    ('gov_nfi',      '6dc7969ac2ec4b1fb1b95027a914acb8', '02eb37ca8d1b430fa27b50d031630c9f'),  # 广义政府
    ('nf_nfi',       'd441e3a39e044d788e3119e68bbb7742', 'd362fde42ab84bf69019d7f53e929344'),  # 非金融企业
    ('fin_nfi',      '354f4f39bade4cfab20137339737d860', '6d02615410d947cc913b56d3dc51846e'),  # 金融机构
    ('row_nfi',      'b6059da73d1f4f1e83ae0715621a9f3c', '55f148c5772d483fb45b350fc23a80b0'),  # 国外
    # ---- 投资与红利 ----
    ('gcf',          '8fbe08322ef841a484438f8c97c9fb91', 'cfd2ee3299a3444b8038600080dbb035'),  # 国内资本形成总额
    ('nf_div_paid',  'd441e3a39e044d788e3119e68bbb7742', '0f9256de3dae418a97a9ae37e4525390'),  # 非金融企业运用红利
    ('nf_div_recv',  'c8cb66f46d314a039e747f426509dbd7', 'a4e0ef34e4e0461caf87171fb8ce15fb'),  # 非金融企业来源红利
    ('fin_div_paid', '354f4f39bade4cfab20137339737d860', '13a7535290f7483ab7b7eb13295c66fb'),  # 金融机构运用红利
    ('fin_div_recv', 'ff99935ac60a4ebe89cda3bd53349ab9', '588e6206087e458f9b5ee4ba7d85f0b4'),  # 金融机构来源红利
    # ---- 名义 GDP（国民经济核算→国内生产总值，用于 %GDP 换算） ----
    ('ngdp',         'f7fd25aaad184414875632cf2327da60', '7dc6a2ee6c614960b7059991e0cc4d96'),
]

# 判定「最新完整年份」时必须齐全的核心变量（红利分项 2000 年前有缺失属正常）
FOF_CORE_VARIABLES = [
    'hh_saving', 'gov_saving', 'nf_saving', 'fin_saving', 'row_saving',
    'hh_nfi', 'gov_nfi', 'nf_nfi', 'fin_nfi', 'row_nfi', 'gcf', 'ngdp',
]

# ---------------------------------------------------------------------------
# 社会消费品零售总额（月度库，via NBS 新版数据发布库 API）
# 路径：月度数据 → 国内贸易 → 社会消费品零售总额（及兄弟叶子）
# dp 口径：1=当期值→monthly_value, 11=同比→monthly_yoy,
#          21=累计值→ytd_value, 12=累计增长→ytd_yoy（同比均为官方可比口径，
#          与绝对值跨年自算不一致，只存公布值、永不自算）
# 历史起点（实测）：total 当期值 1984-01（同比/累计 2000-01）、限上 2011、
#   城乡/餐饮/商品 2010、限上分类 2010/2011、网零旧口径 2018-02～2025-12、
#   新口径 2026-02 起（新旧口径不可比 → 独立 code 存，图上自然断链）
# 1—2 月：2012 年起 1 月四值全空（不建行）、2 月仅累计两列（=1—2月合并值）
# ---------------------------------------------------------------------------
NBS_V2_MONTHLY_ROOT = 'fc982599aa684be7969d7b90b1bd0e84'  # 月度数据根目录 UUID

# (indicator_code, 官方中文名, 叶子目录 cid, {表字段: 指标 iid})
# 由 queryIndicatorsByCid 一次性枚举后固化（2026-07 实测）；跳过的概念：
# 粮油食品饮料烟酒合计（=三子类加总）、服装类（服装鞋帽的子项）、
# 网零吃/穿/用类（过细，仅累计增速）、网上服务零售额（=总额-商品）
RETAIL_INDICATORS = [
    ('total', '社会消费品零售总额', 'd0cb882c7f27443ab6b3ef9421901961', {
        'monthly_value': '1142a3a03e9045959e606a21822641ac',
        'monthly_yoy': 'aaac57d54d2e465d91bc9f3ea1a8618e',
        'ytd_value': '260a1794443b43dd93a59928b12f38af',
        'ytd_yoy': 'e3ca151b53d347b78d1e179e5ebf1d33',
    }),
    ('ls_total', '限上单位消费品零售额', 'd0cb882c7f27443ab6b3ef9421901961', {
        'monthly_value': '97281ec401c14706a7509672902106af',
        'monthly_yoy': 'e576b095205e414c8a21e112792492ba',
        'ytd_value': 'd09f3a9f472a4c87acd4d67866b6cab7',
        'ytd_yoy': 'f83cf22a85664de7b25ab61da994f08d',
    }),
    ('urban', '城镇社会消费品零售总额', 'd5c7d1062a5742c69a02c39650c7c327', {
        'monthly_value': '9ef40e1bd70e4fd1a94005ef9a3b9e6a',
        'monthly_yoy': '0a131939174d4d21885d3ce53cbe147f',
        'ytd_value': '00326273e65f4c1e958c7a800fc77933',
        'ytd_yoy': '93758cc2ed3244daae3d040f28ee7278',
    }),
    ('rural', '乡村社会消费品零售总额', 'd5c7d1062a5742c69a02c39650c7c327', {
        'monthly_value': 'f32d705cc284404e82849c934011d6b0',
        'monthly_yoy': 'dd474a9e7b7745fba458e648f1f013f6',
        'ytd_value': 'd05a22bb2f5f433cbd09a04dbda1f12e',
        'ytd_yoy': 'be68b49a3fe940849299549a2098e02e',
    }),
    ('catering', '餐饮收入', 'd9821f4ad1ec42ebbbd0554efb3e3772', {
        'monthly_value': '446765807521445c8bbe7b7526501dc8',
        'monthly_yoy': '476cfe584e9849c2a2bac63a2fe1dd49',
        'ytd_value': '9172bc0eeb3246ebb803dfe803e23602',
        'ytd_yoy': '7a6941829f2b47dfa6ba6d8190d753db',
    }),
    ('ls_catering', '限上单位餐饮收入', 'd9821f4ad1ec42ebbbd0554efb3e3772', {
        'monthly_value': '24b382aea8224070a3562d8892e9c6d1',
        'monthly_yoy': '4c08c0eb48e0472ab2044c359e0d9a96',
        'ytd_value': 'dbc34e7102244f8eb5611306d100f28d',
        'ytd_yoy': '1c3eead8795e45eab19d7a36e17f46e0',
    }),
    ('goods', '商品零售', 'd9821f4ad1ec42ebbbd0554efb3e3772', {
        'monthly_value': '2d3e611af5214aa480b8a0a4f2c1785d',
        'monthly_yoy': 'd76706323b3743da8b198c7f7d8c6a1c',
        'ytd_value': '29b800dd9efc4f499be46fd3d17f4238',
        'ytd_yoy': 'f005ff68fbd24acebe28820c86857b78',
    }),
    ('ls_goods', '限上单位商品零售类值', 'd9821f4ad1ec42ebbbd0554efb3e3772', {
        'monthly_value': '55c1e5ef6a674368b5eb0d322726ff2d',
        'monthly_yoy': '75aa5bd0cba0413b86fdcb377cbba1fc',
        'ytd_value': '138440036ff1472eb989fb77099edd7d',
        'ytd_yoy': 'a41610ba426c4eedbef574afe181c3d7',
    }),
    ('cat_grain_food', '粮油、食品类商品零售类值', 'a78d7ed67b2b40a58bb28c15c908296e', {
        'monthly_value': 'd22f3466e81243d38f203f7da7c3a9e9',
        'monthly_yoy': 'ba21c628b5e44fa5a9a2a7d7059d6d05',
        'ytd_value': '336abb3ab8b64c5aa1b56221945bb463',
        'ytd_yoy': '88deb006f65941148e4edfa5e2f5ddb2',
    }),
    ('cat_beverage', '饮料类商品零售类值', 'a78d7ed67b2b40a58bb28c15c908296e', {
        'monthly_value': '35be609558574dcb82b3cda90fe3afcd',
        'monthly_yoy': 'f58adc7997d548ba9f6dffb84afa83ae',
        'ytd_value': '393dcc53ed9b46448d86564111947886',
        'ytd_yoy': '2a5ec2085508433f8442441db2c6bafd',
    }),
    ('cat_tobacco', '烟酒类商品零售类值', 'a78d7ed67b2b40a58bb28c15c908296e', {
        'monthly_value': '6af30ac86af04279a9f85223a15877aa',
        'monthly_yoy': '4639a5d6b7594338938c3861fcfd2abe',
        'ytd_value': 'cee31d46461f4f85afbbe2514a1db825',
        'ytd_yoy': '990b475d027348fcbccdc403fce6092c',
    }),
    ('cat_clothing', '服装鞋帽、针、纺织品类商品零售类值', '09caab6a686d4011a34d6615f9077566', {
        'monthly_value': '063e3ce565514fc3a740fb8abf42ec0a',
        'monthly_yoy': '1a594bb095444dafb12f84bd0562c338',
        'ytd_value': '73585919489e42bc99b0059c0ce63667',
        'ytd_yoy': 'c0fe8a7d6be3448596591c748c754b5f',
    }),
    ('cat_cosmetics', '化妆品类商品零售类值', '2368cbdff5b848ec9802956bf471ac06', {
        'monthly_value': '828bd42ab60843378c877c16559f1062',
        'monthly_yoy': '4138ec8753c244ec9ac27312e28dfbe1',
        'ytd_value': '6dbc670b040b47edb2890e3ca376f776',
        'ytd_yoy': '3719950bfea041978dbb3b744b5dce58',
    }),
    ('cat_gold_jewelry', '金银珠宝类商品零售类值', 'e43cfafd0b0744d0a4da096d17786702', {
        'monthly_value': '77c1a35cd28f4de398cdc2eb7769d902',
        'monthly_yoy': '9685ef0e27794f2786fb20b19bcf88fb',
        'ytd_value': 'f48694c9cabf42f48edfbf31346598af',
        'ytd_yoy': '646decda02884f229bfb47d9bbdb53b2',
    }),
    ('cat_daily_use', '日用品类商品零售类值', 'b87cd34f3b594fbab5828f7038e4b6eb', {
        'monthly_value': '50d05e85ee46433c9343a9ad8e8baf28',
        'monthly_yoy': '54b1f685cb07413890763fe8c7aa85e5',
        'ytd_value': 'd0687b2d1d0e4eb099bfea9edf747cf9',
        'ytd_yoy': 'fa11a8bcee294d7fa383587a95bf62d7',
    }),
    ('cat_sports', '体育、娱乐用品类商品零售类值', 'b8879b68afd8499ead417f93527d20eb', {
        'monthly_value': 'a206cec1efa8452581ad042982d10de7',
        'monthly_yoy': 'bf8ad1adc1734a2ba0b5e1e94dd3330d',
        'ytd_value': 'a1e9a29cf9a54b9692d6f67dc0572c57',
        'ytd_yoy': 'fe06b5d752d4477199e09edfad5f2011',
    }),
    ('cat_books', '书报杂志类商品零售类值', 'bf2f054359be47d08b6526e3c4da01c9', {
        'monthly_value': '96fcb420c37d466b9ca562355f19524f',
        'monthly_yoy': '87e41903bc2740d3a00c9af0b4154c2d',
        'ytd_value': 'a482f9f752414b82bf7de382daf36845',
        'ytd_yoy': 'ba17cde4be244dcb9b8747f4b5679693',
    }),
    ('cat_appliance', '家用电器和音像器材类商品零售类值', '4bc877911968450aadee4fe871e5044f', {
        'monthly_value': '033747ece86d47e683f3734da7687eba',
        'monthly_yoy': '3e9f9ca9f4194757b229fcb6c5181283',
        'ytd_value': 'fabe4e49b84e43b9bb0b4ad8ebf95c00',
        'ytd_yoy': '98dcfecf002e47ea804ce6d469aa87b0',
    }),
    ('cat_medicine', '中西药品类商品零售类值', '65d50902a8854f6d8ca6840b7bf3ccdd', {
        'monthly_value': '7ddd94f5a7d5491dbb4349f395128f31',
        'monthly_yoy': 'e518e0730a6e460383f6b11927c10fbc',
        'ytd_value': '489db3067e7c41a88eb46ccccced8b05',
        'ytd_yoy': 'e0ab44c633c64bdcbf2d3473f98adecd',
    }),
    ('cat_office', '文化办公用品类商品零售类值', '6635d2abbf5d4f398150f6cc69a9c68f', {
        'monthly_value': '8588a6bebeda42b3a215e05d73b7b2f8',
        'monthly_yoy': '489970077f0f4926a31b33ea38c5fd01',
        'ytd_value': '65d047b40af74d2f910c6ae780758303',
        'ytd_yoy': 'dec4ef9586464fe8bca890b49784a1d0',
    }),
    ('cat_furniture', '家具类商品零售类值', '77897326002b4d06b41569410d31529a', {
        'monthly_value': '5a9cff40657b445584888a62fd20af45',
        'monthly_yoy': '6c867686326447c1ab5b064cb4f086ce',
        'ytd_value': '1cfa9b6036c14dfbae118eb61f6a05f5',
        'ytd_yoy': '22142d3017ce46a988bfb9586e95e059',
    }),
    ('cat_telecom', '通讯器材类商品零售类值', '036b6b83cfac4f6cb89f4223f5037569', {
        'monthly_value': '5700c1afddff477a9ccd001d82433a8f',
        'monthly_yoy': 'e0afb8c30c1146b293d6a12ef6a0aa81',
        'ytd_value': '5b62e4be58044f2bad19a1b197eaac2c',
        'ytd_yoy': '6ace326ad4404ef1a5b6adde613c6230',
    }),
    ('cat_petroleum', '石油及制品类商品零售类值', '5fa217ef2e1c46fcad38541d64fd05a8', {
        'monthly_value': '39635d8dc00749f5bf002df568dcb568',
        'monthly_yoy': '5bf62872a3db453bb2c3b71804719ce6',
        'ytd_value': '34705ba0f2214aa1832cc00ea97bd80a',
        'ytd_yoy': '680dce8ef6274ab888be14385e81c3c9',
    }),
    ('cat_building', '建筑及装潢材料类商品零售类值', 'cccea173dfe24beeb8ceb5c65702335c', {
        'monthly_value': '21991e6835e54b6d8cad579158dd4a88',
        'monthly_yoy': 'cdda957f3d5b49eaaeda629bd1f46d11',
        'ytd_value': '4a6e1f0f7f5b498e98660291c8260e1c',
        'ytd_yoy': '4448ffb4fa0f44ff881d260ab74c5ab1',
    }),
    ('cat_auto', '汽车类商品零售类值', '5615be125f5e46638d646e1037a07842', {
        'monthly_value': 'c97179dabcaf4a3b8f3fb70a6646456a',
        'monthly_yoy': '6025375b14d140ae873c317e6a12dc6c',
        'ytd_value': '283389cbdc5a410ca2c8ed4ea5b2d939',
        'ytd_yoy': '18cd5205818e446fae43b45e1fd37f44',
    }),
    ('cat_other', '其他商品零售类值', 'c2ad4f502f8c4829b6cd81d49d412c4e', {
        'monthly_value': '210f438c31d644eb84cf2192f11730ee',
        'monthly_yoy': '25b8977775ce4a2190e5c42168d03502',
        'ytd_value': '95753c69620c45898935488268b1a85f',
        'ytd_yoy': '32e67fc3013d49769f19b47afff29c4c',
    }),
    ('online_total_old', '网上零售额', 'fb09cbc680fe4d348c9d05f535ebf77c', {
        'ytd_value': '069baced432b4e1787ad1da50c11e9fc',
        'ytd_yoy': '96d4169857b8478582aa74aa2db0a675',
    }),
    ('online_goods_old', '实物商品网上零售额', 'fb09cbc680fe4d348c9d05f535ebf77c', {
        'ytd_value': '05fbe85e09c842098509c6b29481ab0c',
        'ytd_yoy': 'f12bf45bdfbb47b18147406246c787a7',
    }),
    ('online_total_new', '网上商品和服务零售额', 'ce144b3caaf9498aabcc713e671c1f33', {
        'ytd_value': 'c7ac997262924badbde80e61623defde',
        'ytd_yoy': 'cc2fa77a5bb64f8da3d63688aa0e2d1b',
    }),
    ('online_goods_new', '网上商品零售额', 'ce144b3caaf9498aabcc713e671c1f33', {
        'ytd_value': '000aa31f692c4f1796dc4cbcefa6133d',
        'ytd_yoy': 'ad9696b7ff4a49bbb9e0b40374e74ae7',
    }),
]

# 核心序列所在叶子（总量/城乡/消费类型）全部失败 → 视为 API 不可用转新闻稿；
# 部分核心叶失败 → 整轮 raise 拒写；非核心叶（分类/网零）失败 → warn 降级继续
RETAIL_CORE_CODES = {'total', 'urban', 'rural', 'goods', 'catering'}

# 新闻稿主表行名（_clean_cn_text + 去「、，,」+ 去「其中：」前缀后）→ indicator_code
# None = 已知行、明确不入库（除汽车、节标题等）
RETAIL_RELEASE_LABEL2CODE = {
    '社会消费品零售总额': 'total',
    '除汽车以外的消费品零售额': None,
    '限额以上单位消费品零售额': 'ls_total',
    '城镇': 'urban',
    '乡村': 'rural',
    '餐饮收入': 'catering',
    '限额以上单位餐饮收入': 'ls_catering',
    '商品零售': 'goods',
    '商品零售额': 'goods',
    '限额以上单位商品零售额': 'ls_goods',
    '限额以上单位商品零售类值': 'ls_goods',
    # 网零新旧口径行名不同，天然区分，无需按年份分支
    '网上零售额': 'online_total_old',
    '实物商品网上零售额': 'online_goods_old',
    '网上商品和服务零售额': 'online_total_new',
    '网上商品零售额': 'online_goods_new',
    # 限上 16 分类（发布稿行名顿号在 clean 时去除）
    '粮油食品类': 'cat_grain_food',
    '饮料类': 'cat_beverage',
    '烟酒类': 'cat_tobacco',
    '服装鞋帽针纺织品类': 'cat_clothing',
    '化妆品类': 'cat_cosmetics',
    '金银珠宝类': 'cat_gold_jewelry',
    '日用品类': 'cat_daily_use',
    '体育娱乐用品类': 'cat_sports',
    '书报杂志类': 'cat_books',
    '家用电器和音像器材类': 'cat_appliance',
    '中西药品类': 'cat_medicine',
    '文化办公用品类': 'cat_office',
    '家具类': 'cat_furniture',
    '通讯器材类': 'cat_telecom',
    '石油及制品类': 'cat_petroleum',
    '建筑及装潢材料类': 'cat_building',
    '汽车类': 'cat_auto',
}

RETAIL_FIELDS = ['monthly_value', 'monthly_yoy', 'ytd_value', 'ytd_yoy']


class NBSFetcher(BaseFetcher):
    """Fetches NBS real estate data."""

    # WAF 熔断：连续这么多次"空响应/连接被掐"即认定被限流，中止本轮 v2 请求
    WAF_STRIKE_LIMIT = 5

    def __init__(self):
        super().__init__()
        self._setup_cn_proxy()
        self._waf_strikes = 0
        self._waf_tripped = False
        self._waf_lock = threading.Lock()

    SERIES = [
        {
            'table': 'nbs_real_estate_climate',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS nbs_real_estate_climate (
                    record_date DATE NOT NULL,
                    value DECIMAL(10,2),
                    change_1m DECIMAL(10,6),
                    change_3m DECIMAL(10,6),
                    change_6m DECIMAL(10,6),
                    change_1y DECIMAL(10,6),
                    change_2y DECIMAL(10,6),
                    change_3y DECIMAL(10,6),
                    PRIMARY KEY (record_date)
                )
            """,
            'columns': ['record_date', 'value', 'change_1m', 'change_3m',
                        'change_6m', 'change_1y', 'change_2y', 'change_3y'],
            'strategy': 'truncate',
            'fetch_func': '_fetch_climate',
        },
        {
            'table': 'nbs_house_price',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS nbs_house_price (
                    record_date DATE NOT NULL,
                    city VARCHAR(20) NOT NULL,
                    new_mom DECIMAL(10,2),
                    new_yoy DECIMAL(10,2),
                    used_mom DECIMAL(10,2),
                    used_yoy DECIMAL(10,2),
                    new_90below_mom DECIMAL(10,2),
                    new_90below_yoy DECIMAL(10,2),
                    new_90to144_mom DECIMAL(10,2),
                    new_90to144_yoy DECIMAL(10,2),
                    new_144above_mom DECIMAL(10,2),
                    new_144above_yoy DECIMAL(10,2),
                    used_90below_mom DECIMAL(10,2),
                    used_90below_yoy DECIMAL(10,2),
                    used_90to144_mom DECIMAL(10,2),
                    used_90to144_yoy DECIMAL(10,2),
                    used_144above_mom DECIMAL(10,2),
                    used_144above_yoy DECIMAL(10,2),
                    PRIMARY KEY (record_date, city)
                )
            """,
            'columns': ['record_date', 'city',
                        'new_mom', 'new_yoy', 'used_mom', 'used_yoy',
                        'new_90below_mom', 'new_90below_yoy',
                        'new_90to144_mom', 'new_90to144_yoy',
                        'new_144above_mom', 'new_144above_yoy',
                        'used_90below_mom', 'used_90below_yoy',
                        'used_90to144_mom', 'used_90to144_yoy',
                        'used_144above_mom', 'used_144above_yoy'],
            'strategy': 'upsert',
            'update_columns': ['new_mom', 'new_yoy', 'used_mom', 'used_yoy',
                               'new_90below_mom', 'new_90below_yoy',
                               'new_90to144_mom', 'new_90to144_yoy',
                               'new_144above_mom', 'new_144above_yoy',
                               'used_90below_mom', 'used_90below_yoy',
                               'used_90to144_mom', 'used_90to144_yoy',
                               'used_144above_mom', 'used_144above_yoy'],
            'fetch_func': '_fetch_house_price',
        },
        {
            'table': 'nbs_real_estate_macro',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS nbs_real_estate_macro (
                    record_date DATE NOT NULL,
                    indicator_code VARCHAR(20) NOT NULL,
                    indicator_name VARCHAR(100),
                    value DECIMAL(18,4),
                    PRIMARY KEY (record_date, indicator_code)
                )
            """,
            'columns': ['record_date', 'indicator_code', 'indicator_name', 'value'],
            'strategy': 'truncate',
            'update_columns': ['indicator_name', 'value'],
            'fetch_func': '_fetch_real_estate_macro',
        },
        {
            'table': 'cn_flow_of_funds',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS cn_flow_of_funds (
                    variable VARCHAR(32) NOT NULL,
                    year VARCHAR(10) NOT NULL,
                    value DECIMAL(20,4) NOT NULL,
                    PRIMARY KEY (variable, year)
                )
            """,
            'columns': ['variable', 'year', 'value'],
            'strategy': 'truncate',
            'fetch_func': '_fetch_flow_of_funds',
        },
        {
            'table': 'nbs_retail_sales',
            'create_sql': """
                CREATE TABLE IF NOT EXISTS nbs_retail_sales (
                    record_date DATE NOT NULL,
                    indicator_code VARCHAR(40) NOT NULL,
                    indicator_name VARCHAR(100),
                    monthly_value DECIMAL(18,4) DEFAULT NULL,
                    monthly_yoy DECIMAL(10,2) DEFAULT NULL,
                    ytd_value DECIMAL(18,4) DEFAULT NULL,
                    ytd_yoy DECIMAL(10,2) DEFAULT NULL,
                    PRIMARY KEY (record_date, indicator_code)
                )
            """,
            'columns': ['record_date', 'indicator_code', 'indicator_name',
                        'monthly_value', 'monthly_yoy', 'ytd_value', 'ytd_yoy'],
            'strategy': 'upsert',
            'update_columns': ['indicator_name', 'monthly_value', 'monthly_yoy',
                               'ytd_value', 'ytd_yoy'],
            # NBS 修订=换值不撤值；COALESCE 防 API 抖动的空值刷掉库内好值
            'coalesce_update': True,
            'fetch_func': '_fetch_retail_sales',
        },
    ]

    @staticmethod
    def _to_float(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        if isinstance(val, str):
            val = (val.replace(',', '')
                      .replace('\xa0', '')
                      .replace('\u3000', '')
                      .strip())
            if val in ('', '-', '--', '\u2014', 'nan', 'NaN'):
                return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _clean_cn_text(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ''
        return re.sub(r'[\s\u3000\xa0]+', '', str(val)).strip()

    @staticmethod
    def _nbs_json(resp):
        if resp.status_code != 200:
            snippet = resp.text[:200].replace('\n', ' ')
            raise ValueError(f"NBS API HTTP {resp.status_code}: {snippet}")
        try:
            return resp.json()
        except ValueError as exc:
            snippet = resp.text[:200].replace('\n', ' ')
            raise ValueError(f"NBS API non-JSON response: {snippet}") from exc

    def _easyquery_available(self, probe):
        attr = f'_easyquery_{probe}_disabled'
        if getattr(self, attr, False):
            return False
        try:
            self._nbs_macro_query('A0601', period='LAST1')
            return True
        except Exception as exc:
            setattr(self, attr, True)
            self.logger.warning(
                f"  NBS easyquery unavailable for {probe}; using official releases: {exc}")
            return False

    def _fetch_release_links(self, keyword, max_pages=8):
        """Return unique NBS news-release links whose title contains keyword."""
        links = []
        seen = set()
        for page_idx in range(max_pages):
            suffix = '' if page_idx == 0 else f'index_{page_idx}.html'
            page_url = urljoin(NBS_RELEASE_LIST_URL, suffix)
            resp = self.session.get(
                page_url, headers=NBS_RELEASE_HEADERS, timeout=30)
            resp.raise_for_status()
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                title = (a.get('title') or a.get_text(' ', strip=True) or '').strip()
                if keyword not in title:
                    continue
                full_url = urljoin(page_url, a['href'])
                if full_url in seen:
                    continue
                seen.add(full_url)
                links.append((title, full_url))
            self.rate_limit_pause(0.2)
        return links

    @staticmethod
    def _parse_house_release_period(title):
        m = re.search(r'(\d{4})\u5e74(\d{1,2})\u6708(?:\u4efd)?70\u4e2a', title)
        if not m:
            return None
        return date(int(m.group(1)), int(m.group(2)), 1)

    @staticmethod
    def _parse_macro_release_period(title):
        year_m = re.search(r'(\d{4})\u5e74', title)
        if not year_m:
            return None
        year = int(year_m.group(1))
        m = re.search(r'1[\u2014\u2013-](\d{1,2})\u6708', title)
        if m:
            return date(year, int(m.group(1)), 1)
        if '\u4e00\u5b63\u5ea6' in title:
            return date(year, 3, 1)
        if '\u4e0a\u534a\u5e74' in title:
            return date(year, 6, 1)
        if '\u524d\u4e09\u5b63\u5ea6' in title:
            return date(year, 9, 1)
        if re.search(r'\d{4}\u5e74\u5168\u56fd\u623f\u5730\u4ea7', title):
            return date(year, 12, 1)
        return None

    # -- Climate index (AKShare) --------------------------------------------

    def _fetch_climate(self):
        """Fetch real estate climate index via ak.macro_china_real_estate()."""
        df = self.retry_call(ak.macro_china_real_estate, label='macro_china_real_estate')
        rows = []
        for _, r in df.iterrows():
            d = pd.to_datetime(r.iloc[0], errors='coerce')
            if pd.isna(d):
                continue
            rows.append((
                d.date(),
                self._to_float(r.iloc[1]),
                self._to_float(r.iloc[2]),
                self._to_float(r.iloc[3]),
                self._to_float(r.iloc[4]),
                self._to_float(r.iloc[5]),
                self._to_float(r.iloc[6]),
                self._to_float(r.iloc[7]),
            ))
        return rows

    # -- NBS v2 API 统一入口（带 WAF 熔断） -----------------------------------

    def _waf_strike(self, why):
        """记一次 WAF 特征失败；连续 WAF_STRIKE_LIMIT 次即跳闸。"""
        with self._waf_lock:
            self._waf_strikes += 1
            strikes = self._waf_strikes
            just_tripped = (strikes >= self.WAF_STRIKE_LIMIT
                            and not self._waf_tripped)
            if just_tripped:
                self._waf_tripped = True
        self.logger.warning(
            f"  WAF strike {strikes}/{self.WAF_STRIKE_LIMIT}: {why}")
        if just_tripped:
            self.logger.error(
                "  NBS v2 WAF throttle detected — aborting all remaining v2 "
                "requests this run; next scheduled run will retry")

    def _v2_post_json(self, payload, label, pause=0.0):
        """POST {NBS_V2_BASE}/stream/esData 并解析 JSON，带 WAF 熔断。

        限流特征（空 body → JSON 在 char 0 解析失败、连接被远端掐断、
        403/429）计 strike；连续 WAF_STRIKE_LIMIT 次后跳闸，此后所有 v2
        请求立即抛 NBSThrottledError（AbortFetch 子类，retry_call 不再重试
        也不再 backoff 睡眠）。新闻稿回退走 www.stats.gov.cn，不受影响。
        """
        with self._waf_lock:
            if self._waf_tripped:
                raise NBSThrottledError(
                    f"{label}: skipped — NBS v2 WAF throttled earlier in this run")
        if pause:
            self.rate_limit_pause(pause)
        try:
            r = self.session.post(f'{NBS_V2_BASE}/stream/esData', json=payload,
                                  headers=NBS_V2_HEADERS, timeout=120,
                                  verify=False)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.RetryError) as e:
            self._waf_strike(f"{label}: connection dropped "
                             f"({e.__class__.__name__})")
            raise
        if r.status_code in (403, 429):
            self._waf_strike(f"{label}: HTTP {r.status_code}")
            raise ValueError(f"NBS v2 {label} HTTP {r.status_code} (WAF?)")
        if r.status_code != 200:
            snippet = r.text[:200].replace('\n', ' ')
            raise ValueError(f"NBS v2 {label} HTTP {r.status_code}: {snippet}")
        try:
            data = r.json()
        except ValueError:
            self._waf_strike(f"{label}: empty/non-JSON body")
            raise ValueError(
                f"NBS v2 {label}: empty/non-JSON response (WAF throttle?)")
        with self._waf_lock:
            self._waf_strikes = 0
        return data

    # -- 70-city house price (NBS 新版数据发布库 API) ---------------------------

    def _nbs_v2_house_city(self, city, code6, dts):
        """单城市取 16 个房价指标。返回 {date: {column: float}}。

        das 只支持单城市；无效码/多城市会被服务端【静默回退成北京】，
        故必须校验 da_name 回显（2026-03 数据污染事故根因，勿删）。
        """
        payload = {
            'cid': HP_V2_CID,
            'indicatorIds': list(HP_V2_INDICATORS.values()),
            'daCatalogId': '',
            'das': [{'text': city, 'value': code6 + '000000'}],
            'showType': '1',
            'dts': [dts],
            'rootId': NBS_V2_CITYPRICE_ROOT,
        }
        # 网宿 WAF 对突发请求限流（返回非 JSON），必须控制节奏
        data = self._v2_post_json(payload, f'hp_{city}', pause=0.5)
        if data.get('state') != 20000:
            raise ValueError(f"NBS v2 esData error: {str(data)[:200]}")

        iid_to_col = {iid: col for col, iid in HP_V2_INDICATORS.items()}
        out = {}
        for block in data.get('data') or []:
            code = block.get('code') or ''
            try:
                record_date = pd.to_datetime(code[:6], format='%Y%m').date()
            except Exception:
                continue
            for v in block.get('values') or []:
                da_name = v.get('da_name') or ''
                if not da_name.startswith(city):
                    raise ValueError(
                        f"NBS v2 returned region {da_name!r} for requested {city!r} "
                        f"(silent-default footgun)")
                col = iid_to_col.get(v.get('_id'))
                val = self._to_float(v.get('value'))
                if col is None or val is None:
                    continue
                out.setdefault(record_date, {})[col] = val
        return out

    def _overall_house_block_starts(self, table):
        headers = [self._clean_cn_text(v) for v in table.iloc[0]]
        starts = [i for i, h in enumerate(headers) if h == '\u57ce\u5e02']
        if len(starts) >= 2:
            return starts[:2]

        # Older/no-average releases have 6 columns; current YTD-average
        # releases have 8. If the header is malformed, split the table in two.
        half_width = len(headers) // 2
        if half_width >= 3:
            return [0, half_width]
        return [0]

    def _add_overall_house_table(self, table, rows_by_city, mom_col, yoy_col):
        block_starts = self._overall_house_block_starts(table)
        for _, r in table.iterrows():
            for base in block_starts:
                if base + 2 >= len(r):
                    continue
                city = self._clean_cn_text(r.iloc[base])
                if not city or city == '\u57ce\u5e02':
                    continue
                mom = self._to_float(r.iloc[base + 1])
                yoy = self._to_float(r.iloc[base + 2])
                if mom is None and yoy is None:
                    continue
                rows_by_city.setdefault(city, {})[mom_col] = mom
                rows_by_city.setdefault(city, {})[yoy_col] = yoy

    def _area_house_group_starts(self, table):
        headers = [self._clean_cn_text(v) for v in table.iloc[0]]
        starts = []
        prev = None
        for i, header in enumerate(headers[1:], start=1):
            if not header or header == prev:
                continue
            starts.append(i)
            prev = header
        if len(starts) >= 3:
            return starts[:3]

        metric_width = (len(headers) - 1) // 3
        if metric_width >= 2:
            return [1, 1 + metric_width, 1 + metric_width * 2]
        return []

    def _add_area_house_table(self, table, rows_by_city, prefix):
        group_starts = self._area_house_group_starts(table)
        if len(group_starts) < 3:
            raise ValueError(f"Cannot detect house-price area table layout: {table.shape}")
        suffixes = ['90below', '90to144', '144above']

        for _, r in table.iterrows():
            city = self._clean_cn_text(r.iloc[0])
            if not city or city == '\u57ce\u5e02':
                continue
            vals = rows_by_city.setdefault(city, {})
            for suffix, start in zip(suffixes, group_starts):
                if start + 1 >= len(r):
                    continue
                mom = self._to_float(r.iloc[start])
                yoy = self._to_float(r.iloc[start + 1])
                if mom is None and yoy is None:
                    continue
                vals[f'{prefix}_{suffix}_mom'] = mom
                vals[f'{prefix}_{suffix}_yoy'] = yoy

    def _fetch_house_price_release(self, title, url):
        record_date = self._parse_house_release_period(title)
        if not record_date:
            return []
        resp = self.session.get(url, headers=NBS_RELEASE_HEADERS, timeout=60)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
        tables = pd.read_html(StringIO(resp.text))
        if len(tables) < 6:
            raise ValueError(f"House price release has {len(tables)} tables: {url}")

        rows_by_city = {}
        self._add_overall_house_table(tables[0], rows_by_city, 'new_mom', 'new_yoy')
        self._add_overall_house_table(tables[1], rows_by_city, 'used_mom', 'used_yoy')
        self._add_area_house_table(tables[2], rows_by_city, 'new')
        self._add_area_house_table(tables[3], rows_by_city, 'new')
        self._add_area_house_table(tables[4], rows_by_city, 'used')
        self._add_area_house_table(tables[5], rows_by_city, 'used')

        col_order = [
            'new_mom', 'new_yoy', 'used_mom', 'used_yoy',
            'new_90below_mom', 'new_90below_yoy',
            'new_90to144_mom', 'new_90to144_yoy',
            'new_144above_mom', 'new_144above_yoy',
            'used_90below_mom', 'used_90below_yoy',
            'used_90to144_mom', 'used_90to144_yoy',
            'used_144above_mom', 'used_144above_yoy',
        ]
        rows = []
        for city, vals in rows_by_city.items():
            row = [record_date, city]
            row.extend(vals.get(col) for col in col_order)
            if any(v is not None for v in row[2:]):
                rows.append(tuple(row))
        if len(rows) < 60:
            raise ValueError(f"Only parsed {len(rows)} house price city rows from {url}")
        self.logger.info(f"  Official release {record_date}: {len(rows)} house-price rows")
        return rows

    def _fetch_house_price_releases(self, after_date=None):
        keyword = '70\u4e2a\u5927\u4e2d\u57ce\u5e02\u5546\u54c1\u4f4f\u5b85\u9500\u552e\u4ef7\u683c\u53d8\u52a8\u60c5\u51b5'
        releases = []
        for title, url in self._fetch_release_links(keyword, max_pages=8):
            record_date = self._parse_house_release_period(title)
            if not record_date:
                continue
            if after_date and record_date <= after_date:
                continue
            releases.append((record_date, title, url))
        rows = []
        for _, title, url in sorted(releases):
            rows.extend(self._fetch_house_price_release(title, url))
            self.rate_limit_pause(0.5)
        return rows

    def _fetch_house_price(self, period='LAST180', latest_date=None):
        """Fetch all 70-city house price indicators via NBS v2 API（主要城市月度价格库）.
        Makes 70 API calls (one per city, 16 indicators each) in parallel (5 threads).
        Period: LAST13 for incremental, LAST180 for full refresh (2011-01 起).
        任一城市失败则整体 raise——upsert 无法回滚，宁可整轮不写也不留残缺。
        """
        today = date.today()
        if period == 'LAST13' and latest_date:
            start = (pd.Timestamp(latest_date) - pd.DateOffset(months=13)).date()
        else:
            start = date(2011, 1, 1)
        dts = f"{start:%Y%m}MM-{today:%Y%m}MM"

        # 可用性探针：v2 不可达时直接走新闻稿回退，别让 70 城各自重试到超时
        try:
            self.retry_call(
                lambda: self._nbs_v2_house_city('北京', '110000',
                                                f"{today:%Y%m}MM-{today:%Y%m}MM"),
                max_retries=3, backoff=5, label='nbs_hp_probe')
            api_ok = True
        except Exception as exc:
            self.logger.warning(
                f"  NBS v2 city-price API unavailable; using official releases: {exc}")
            api_ok = False

        rows = []
        errors = []
        api_cities = 0
        if api_ok:
            # 并发别超过 2：WAF 会对突发流量整段封锁（linear backoff 给冷却期）
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = {
                    executor.submit(
                        self.retry_call,
                        lambda c=city, k=code, d=dts: self._nbs_v2_house_city(c, k, d),
                        max_retries=4,
                        backoff=10,
                        label=f'nbs_hp_{city}',
                    ): city
                    for city, code in HP_CITIES
                }
                for future in as_completed(futures):
                    city = futures[future]
                    try:
                        city_data = future.result()
                        for record_date, vals in sorted(city_data.items()):
                            row = [record_date, city]
                            row.extend(vals.get(col) for col in HP_COL_ORDER)
                            if any(v is not None for v in row[2:]):
                                rows.append(tuple(row))
                        api_cities += 1
                    except Exception as e:
                        if isinstance(e, NBSThrottledError):
                            self.logger.warning(f"  {city}: skipped (WAF throttled)")
                        else:
                            self.logger.error(f"  {city} FAILED: {e}")
                        errors.append((city, e))

            if errors:
                # upsert 不可回滚：部分城市成功、部分失败时宁可整轮放弃
                failed = ', '.join(city for city, _exc in errors)
                raise RuntimeError(
                    f"Partial NBS house-price fetch failed for "
                    f"{len(errors)} city/cities: {failed}")

        self.logger.info(f"  Total: {len(rows)} rows across {api_cities} cities")
        max_date = max((r[0] for r in rows), default=None)
        release_after = max_date or (latest_date - timedelta(days=1) if latest_date else None)
        try:
            release_rows = self._fetch_house_price_releases(after_date=release_after)
        except Exception as exc:
            if not rows:
                raise
            self.logger.warning(f"  Official NBS release fallback failed: {exc}")
            release_rows = []
        if release_rows:
            rows.extend(release_rows)
            self.logger.info(f"  Added {len(release_rows)} rows from official NBS releases")
        if not rows:
            raise RuntimeError("No NBS house price rows fetched")
        return rows

    # -- National macro real estate data (NBS easyquery API, hgyd) -----------

    def _nbs_macro_query(self, zb_code, period='LAST20'):
        """Query NBS easyquery API for national macro indicators (dbcode=hgyd).
        Returns list of (period_str, indicator_code, indicator_name, value).
        """
        params = {
            'm': 'QueryData',
            'dbcode': 'hgyd',
            'rowcode': 'zb',
            'colcode': 'sj',
            'wds': '[]',
            'dfwds': json.dumps([
                {'wdcode': 'zb', 'valuecode': zb_code},
                {'wdcode': 'sj', 'valuecode': period},
            ]),
            'k1': str(int(time.time() * 1000)),
        }
        r = requests.get(NBS_API_URL, params=params, timeout=120, verify=False,
                         headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'},
                         proxies=self._cn_proxies)
        data = self._nbs_json(r)
        if data.get('returncode') != 200:
            raise ValueError(f"NBS API error: {data.get('returncode')}")

        # Build indicator name mapping
        zb_names = {}
        for w in data['returndata']['wdnodes']:
            if w.get('wdcode') == 'zb':
                for node in w['nodes']:
                    zb_names[node['code']] = node['cname']

        results = []
        for n in data['returndata']['datanodes']:
            wds_map = {wd['wdcode']: wd['valuecode'] for wd in n['wds']}
            zb = wds_map.get('zb', '')
            sj = wds_map.get('sj', '')
            val = n['data']['data'] if n['data']['hasdata'] else None
            results.append((sj, zb, zb_names.get(zb, zb), val))

        return results

    def _macro_release_codes(self, label, parent):
        if label.startswith('\u623f\u5730\u4ea7\u5f00\u53d1\u6295\u8d44'):
            return 'investment', ('A060101', 'A060102')
        if label.startswith('\u529e\u516c\u697c') and parent == 'investment':
            return parent, ('A06010D', 'A06010E')
        if label.startswith('\u5546\u4e1a\u8425\u4e1a\u7528\u623f') and parent == 'investment':
            return parent, ('A06010F', 'A06010G')
        if label.startswith('\u623f\u5c4b\u65bd\u5de5\u9762\u79ef'):
            return 'construction', ('A060401', 'A060402')
        if label.startswith('\u623f\u5c4b\u65b0\u5f00\u5de5\u9762\u79ef'):
            return 'new_starts', ('A060403', 'A060404')
        if label.startswith('\u623f\u5c4b\u7ae3\u5de5\u9762\u79ef'):
            return 'completions', ('A060405', 'A060406')
        if label.startswith('\u65b0\u5efa\u5546\u54c1\u623f\u9500\u552e\u9762\u79ef'):
            return 'sales_area', ('A060801', 'A060802')
        if label.startswith('\u65b0\u5efa\u5546\u54c1\u623f\u9500\u552e\u989d'):
            return 'sales_value', ('A060901', 'A060902')
        if label.startswith('\u623f\u5730\u4ea7\u5f00\u53d1\u4f01\u4e1a\u672c\u5e74\u5230\u4f4d\u8d44\u91d1'):
            return 'funds', ('A060205', 'A060206')
        if label.startswith('\u5546\u54c1\u623f\u5f85\u552e\u9762\u79ef'):
            return 'inventory', None
        if label.startswith('\u5176\u4e2d\uff1a\u56fd\u5185\u8d37\u6b3e') and parent == 'funds':
            return parent, ('A060207', 'A060208')
        if label.startswith('\u56fd\u5185\u8d37\u6b3e') and parent == 'funds':
            return parent, ('A060207', 'A060208')
        if label.startswith('\u81ea\u7b79\u8d44\u91d1') and parent == 'funds':
            return parent, ('A06020B', 'A06020C')
        if label in ('\u5176\u4e2d\uff1a\u4f4f\u5b85', '\u4f4f\u5b85'):
            res_codes = {
                'investment': ('A060105', 'A060106'),
                'construction': ('A060501', 'A060502'),
                'new_starts': ('A060503', 'A060504'),
                'completions': ('A060505', 'A060506'),
                'sales_area': ('A060A01', 'A060A02'),
                'sales_value': ('A060B01', 'A060B02'),
            }
            return parent, res_codes.get(parent)
        return parent, None

    def _fetch_macro_release(self, title, url):
        record_date = self._parse_macro_release_period(title)
        if not record_date:
            return []
        resp = self.session.get(url, headers=NBS_RELEASE_HEADERS, timeout=60)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
        tables = pd.read_html(StringIO(resp.text))
        source_table = None
        for table in tables:
            header = self._clean_cn_text(table.iloc[0, 0]) if table.shape[1] else ''
            if table.shape[1] >= 3 and '\u6307\u6807' in header:
                source_table = table
                break
        if source_table is None:
            raise ValueError(f"Macro release table not found: {url}")

        rows = []
        parent = None
        for _, r in source_table.iloc[1:].iterrows():
            label = self._clean_cn_text(r.iloc[0])
            if not label:
                continue
            parent, codes = self._macro_release_codes(label, parent)
            if not codes:
                continue
            value = self._to_float(r.iloc[1])
            yoy = self._to_float(r.iloc[2])
            level_code, yoy_code = codes
            rows.append((record_date, level_code, label, value))
            rows.append((record_date, yoy_code, f'{label}_\u7d2f\u8ba1\u589e\u957f', yoy))
        if len(rows) < 20:
            raise ValueError(f"Only parsed {len(rows)} macro rows from {url}")
        self.logger.info(f"  Official release {record_date}: {len(rows)} macro rows")
        return rows

    def _fetch_macro_releases(self, after_date=None):
        keyword = '\u5168\u56fd\u623f\u5730\u4ea7\u5e02\u573a\u57fa\u672c\u60c5\u51b5'
        releases = []
        for title, url in self._fetch_release_links(keyword, max_pages=8):
            record_date = self._parse_macro_release_period(title)
            if not record_date:
                continue
            if after_date and record_date <= after_date:
                continue
            releases.append((record_date, title, url))
        rows = []
        for _, title, url in sorted(releases):
            rows.extend(self._fetch_macro_release(title, url))
            self.rate_limit_pause(0.5)
        return rows

    def _fetch_one_macro_category(self, zb_code, category_name):
        """Fetch one macro category. Called in parallel by _fetch_real_estate_macro."""
        results = self.retry_call(
            lambda code=zb_code: self._nbs_macro_query(code, period='LAST180'),
            max_retries=2,
            backoff=2,
            label=f'nbs_macro_{zb_code}',
        )
        rows = []
        for sj, indicator_code, indicator_name, val in results:
            try:
                record_date = pd.to_datetime(sj, format='%Y%m').date()
            except Exception:
                continue
            rows.append((record_date, indicator_code, indicator_name,
                         self._to_float(val)))
        self.logger.info(f"  {zb_code} ({category_name}): {len(rows)} values")
        return rows

    def _fetch_real_estate_macro(self, latest_date=None):
        """Fetch all national macro real estate indicators (9 categories) in parallel (5 threads)."""
        rows = []
        errors = []
        items = list(NBS_MACRO_CATEGORIES.items())
        if self._easyquery_available('macro'):
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(self._fetch_one_macro_category, zb_code, cat_name): (zb_code, cat_name)
                    for zb_code, cat_name in items
                }
                for future in as_completed(futures):
                    zb_code, cat_name = futures[future]
                    try:
                        cat_rows = future.result()
                        rows.extend(cat_rows)
                    except Exception as e:
                        if isinstance(e, NBSThrottledError):
                            self.logger.warning(f"  {zb_code} ({cat_name}): skipped (WAF throttled)")
                        else:
                            self.logger.error(f"  {zb_code} ({cat_name}) FAILED: {e}")
                        errors.append((zb_code, e))

        if errors:
            failed = ', '.join(code for code, _exc in errors)
            raise RuntimeError(
                f"Partial NBS macro fetch failed for "
                f"{len(errors)} category/categories: {failed}"
            )

        self.logger.info(f"  Total: {len(rows)} macro indicator rows")
        max_date = max((r[0] for r in rows), default=None)
        release_after = max_date or (latest_date - timedelta(days=1) if latest_date else None)
        try:
            release_rows = self._fetch_macro_releases(after_date=release_after)
        except Exception as exc:
            if not rows:
                raise
            self.logger.warning(f"  Official NBS release fallback failed: {exc}")
            release_rows = []
        if release_rows:
            rows.extend(release_rows)
            self.logger.info(f"  Added {len(release_rows)} rows from official NBS releases")
        if not rows:
            if errors:
                raise RuntimeError(f"No NBS macro rows fetched; {len(errors)} category fetches failed")
            raise RuntimeError("No NBS macro rows fetched")
        if not max_date and release_rows:
            self._last_fetch_partial = True
        return rows

    # -- Flow of funds (NBS 新版数据发布库 API) --------------------------------

    def _nbs_v2_esdata(self, cid, indicator_ids, dts, root_id=NBS_V2_ANNUAL_ROOT):
        """POST /stream/esData：按叶子目录 cid + 指标 UUID 列表批量取值。

        dts 写法：年度 '1992YY-2026YY'，月度 '198401MM-202607MM'。
        root_id 须与 cid 所在数据库一致（年度/月度根不同）。

        Returns:
            list of period blocks: [{'code': '2023YY'|'202605MM', 'values': [{'_id', 'value', ...}]}]
        """
        payload = {
            'cid': cid,
            'indicatorIds': indicator_ids,
            'daCatalogId': '',
            'das': [{'text': '全国', 'value': '000000000000'}],
            'showType': '1',
            'dts': [dts],
            'rootId': root_id,
        }
        data = self._v2_post_json(payload, f'esData_{cid[:8]}')
        if data.get('state') != 20000:
            raise ValueError(f"NBS v2 esData error: {str(data)[:200]}")
        return data['data']

    def _fof_latest_complete_year_from_db(self, conn):
        cursor = conn.cursor()
        try:
            placeholders = ', '.join(['%s'] * len(FOF_CORE_VARIABLES))
            cursor.execute(
                f"""
                SELECT `year` FROM cn_flow_of_funds
                WHERE variable IN ({placeholders})
                GROUP BY `year`
                HAVING COUNT(DISTINCT variable) = %s
                ORDER BY CAST(`year` AS UNSIGNED) DESC
                LIMIT 1
                """,
                (*FOF_CORE_VARIABLES, len(FOF_CORE_VARIABLES)),
            )
            row = cursor.fetchone()
            return int(row[0]) if row else None
        except Exception:
            return None
        finally:
            cursor.close()

    def _fetch_flow_of_funds(self):
        """Fetch 资金流量表（非金融交易）annual series 1992+ plus nominal GDP."""
        dts = f'1992YY-{date.today().year}YY'

        # 按叶子目录分组，减少请求次数
        by_cid = {}
        for variable, cid, iid in FOF_INDICATORS:
            by_cid.setdefault(cid, []).append((variable, iid))

        series = {}  # variable -> {year(int): float}
        for cid, pairs in by_cid.items():
            ids = [iid for _, iid in pairs]
            id2var = {iid: variable for variable, iid in pairs}
            blocks = self.retry_call(
                lambda c=cid, i=ids: self._nbs_v2_esdata(c, i, dts),
                max_retries=3,
                backoff=3,
                label=f'nbs_v2_esdata {cid[:8]}',
            )
            for blk in blocks:
                year_str = str(blk.get('code', '')).replace('YY', '')
                if not year_str.isdigit():
                    continue
                for v in blk.get('values', []):
                    variable = id2var.get(v.get('_id'))
                    if variable is None:
                        continue
                    val = self._to_float(v.get('value'))
                    if val is None:
                        continue
                    series.setdefault(variable, {})[int(year_str)] = val
            self.rate_limit_pause(0.4)

        missing = [v for v, _, _ in FOF_INDICATORS if not series.get(v)]
        if missing:
            raise RuntimeError(
                f"NBS v2 FoF response missing variables: {', '.join(missing)}")

        # 最新完整年份：所有核心变量齐全的最大年份；防止半截数据覆盖完整数据
        complete_years = None
        for variable in FOF_CORE_VARIABLES:
            years = set(series[variable])
            complete_years = years if complete_years is None else complete_years & years
        if not complete_years:
            raise RuntimeError("NBS v2 FoF: no complete year across core variables")
        latest = max(complete_years)
        if len(complete_years) < 25:
            raise RuntimeError(
                f"NBS v2 FoF: only {len(complete_years)} complete years "
                f"(expected ~30+); refusing to overwrite")

        conn = self.get_connection()
        try:
            existing_latest = self._fof_latest_complete_year_from_db(conn)
        finally:
            conn.close()
        if existing_latest and latest < existing_latest:
            raise RuntimeError(
                f"NBS v2 FoF latest complete year would regress from "
                f"{existing_latest} to {latest}; refusing to overwrite")

        self.logger.info(
            f"  FoF: {len(complete_years)} complete years, latest {latest}")

        rows = []
        for variable, years in series.items():
            for year, val in years.items():
                rows.append((variable, str(year), val))
        return rows

    # -- Retail sales 社会消费品零售总额 (NBS v2 月度库 + 官方新闻稿回退) -----

    @staticmethod
    def _retail_value_ok(field, val):
        """值域护栏：亿元 (0, 1e6)，百分比 [-100, 300]。"""
        if field.endswith('_value'):
            return 0 < val < 1e6
        return -100 <= val <= 300

    @staticmethod
    def _parse_retail_release_period(title):
        """「2026年1—5月份社会消费品零售总额增长1.4%」/「2026年3月份…下降…」→ 当月 1 日。

        容忍 增长/下降/持平 等任意结尾；「1—M月份」取 M；单独「1月份」不存在，
        防御性返回 None（1 月无单独发布，避免误建 1 月行）。
        """
        title = re.sub(r'\s+', '', title)
        named = re.search(r'(\d{4})年(上半年|前三季度|一季度|全年)社会消费品零售总额', title)
        if named:
            month = {'上半年': 6, '前三季度': 9, '一季度': 3, '全年': 12}[named[2]]
            return date(int(named[1]), month, 1)
        m = re.search(r'(\d{4})年(1[—–-])?(\d{1,2})月份?'
                      r'社会消费品零售总额', title)
        if not m:
            return None
        year, combined, month = int(m.group(1)), m.group(2), int(m.group(3))
        if not 1 <= month <= 12:
            return None
        if month == 1 and not combined:
            return None
        return date(year, month, 1)

    @classmethod
    def _retail_release_label(cls, raw):
        """新闻稿行名归一化：去空白/序号/「其中：」/顿号逗号，供 LABEL2CODE 查表。"""
        label = cls._clean_cn_text(raw)
        label = re.sub(r'^[（(][一二三四五六七八九十]+[)）]', '', label)
        label = re.sub(r'^其中[：:]', '', label)
        label = label.lstrip('#')
        return label.replace('、', '').replace('，', '').replace(',', '')

    def _fetch_retail_release(self, title, url):
        """解析一期社零发布稿主表 → {(record_date, code): {field: value}}。

        正常月份 5 列（当月绝对量/当月同比/累计绝对量/累计同比），
        1—2 月合并期 3 列（仅累计两列）。页面含重复表副本，取首张含「指标」的表。
        """
        record_date = self._parse_retail_release_period(title)
        if not record_date:
            return {}
        resp = self.session.get(url, headers=NBS_RELEASE_HEADERS, timeout=60)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
        tables = pd.read_html(StringIO(resp.text))
        source_table = None
        for table in tables:
            header = self._clean_cn_text(table.iloc[0, 0]) if table.shape[1] else ''
            if table.shape[1] >= 3 and '指标' in header:
                source_table = table
                break
        if source_table is None:
            raise ValueError(f"Retail release table not found: {url}")

        ncols = source_table.shape[1]
        cells = {}
        unmapped = []
        for _, r in source_table.iloc[1:].iterrows():
            label = self._retail_release_label(r.iloc[0])
            if (not label or label.startswith('指标')
                    or label.startswith('注')):
                continue
            if '按经营地' in label or '按消费类型' in label:
                continue
            if label not in RETAIL_RELEASE_LABEL2CODE:
                unmapped.append(label)
                continue
            code = RETAIL_RELEASE_LABEL2CODE[label]
            if code is None:
                continue
            if ncols >= 5:
                fields = {
                    'monthly_value': self._to_float(r.iloc[1]),
                    'monthly_yoy': self._to_float(r.iloc[2]),
                    'ytd_value': self._to_float(r.iloc[3]),
                    'ytd_yoy': self._to_float(r.iloc[4]),
                }
            else:
                fields = {
                    'ytd_value': self._to_float(r.iloc[1]),
                    'ytd_yoy': self._to_float(r.iloc[2]),
                }
            fields = {f: v for f, v in fields.items()
                      if v is not None and self._retail_value_ok(f, v)}
            if not fields:
                continue
            tgt = cells.setdefault((record_date, code), {})
            for f, v in fields.items():
                if tgt.get(f) is None:
                    tgt[f] = v
        if unmapped:
            self.logger.warning(
                f"  Retail release unmapped rows ({record_date}): {unmapped}")
        if len(cells) < 15:
            raise ValueError(f"Only mapped {len(cells)} retail rows from {url}")
        self.logger.info(
            f"  Official release {record_date}: {len(cells)} retail series")
        return cells

    def _fetch_retail_releases(self, after_date=None, max_pages=8):
        """抓取比 after_date 新的所有社零发布稿，合并为 cell 字典。

        单篇解析失败只跳过该篇（warn），不放弃其余发布稿；若所有篇目都
        失败则返回空 dict，由调用方的 no-rows 护栏决定是否报错。
        """
        keyword = '社会消费品零售总额'
        releases = []
        for title, url in self._fetch_release_links(keyword, max_pages=max_pages):
            record_date = self._parse_retail_release_period(title)
            if not record_date:
                continue
            if after_date and record_date <= after_date:
                continue
            releases.append((record_date, title, url))
        cells = {}
        for _, title, url in sorted(releases):
            try:
                release_cells = self._fetch_retail_release(title, url)
            except Exception as exc:
                self.logger.warning(
                    f"  Retail release parse failed, skipped ({title}): {exc}")
                continue
            for key, fields in release_cells.items():
                tgt = cells.setdefault(key, {})
                for f, v in fields.items():
                    if tgt.get(f) is None:
                        tgt[f] = v
            self.rate_limit_pause(0.5)
        return cells

    def _fetch_retail_sales(self, latest_date=None):
        """社零全指标（30 codes × 4 口径）。

        主通道 v2 API：增量回看 24 个月（覆盖上年 1—2 月的基数修订），
        表空或 NBS_RETAIL_FULL=1 时全量 1984 起。核心叶（总量/城乡/消费类型）
        部分失败整轮 raise；全部失败转新闻稿灾备；非核心叶失败降级继续。
        新闻稿常态补最新期（发布日 API 上库可能滞后）。
        """
        full = latest_date is None or os.environ.get('NBS_RETAIL_FULL') == '1'
        today = date.today()
        if full:
            dts = f'198401MM-{today:%Y%m}MM'
        else:
            back = latest_date.year * 12 + latest_date.month - 1 - 24
            dts = f'{date(back // 12, back % 12 + 1, 1):%Y%m}MM-{today:%Y%m}MM'
        self.logger.info(
            f"  Retail window {dts} ({'full' if full else 'incremental'})")

        by_cid = {}
        for code, name, cid, fields in RETAIL_INDICATORS:
            by_cid.setdefault(cid, []).append((code, fields))
        core_cids = {cid for code, _n, cid, _f in RETAIL_INDICATORS
                     if code in RETAIL_CORE_CODES}
        code_names = {code: name for code, name, _c, _f in RETAIL_INDICATORS}

        cells = {}  # (record_date, code) -> {field: float}
        core_fail = 0
        for cid, group in by_cid.items():
            iid_map = {}
            for code, fields in group:
                for field, iid in fields.items():
                    iid_map[iid] = (code, field)
            try:
                blocks = self.retry_call(
                    lambda c=cid, ids=list(iid_map): self._nbs_v2_esdata(
                        c, ids, dts, root_id=NBS_V2_MONTHLY_ROOT),
                    max_retries=3, backoff=3, label=f'nbs_v2_retail {cid[:8]}')
            except Exception as exc:
                if cid in core_cids:
                    core_fail += 1
                if isinstance(exc, NBSThrottledError):
                    self.logger.warning(f"  Retail leaf {cid[:8]}: skipped (WAF throttled)")
                elif cid in core_cids:
                    self.logger.error(f"  Retail CORE leaf {cid[:8]} failed: {exc}")
                else:
                    self.logger.warning(
                        f"  Retail leaf {cid[:8]} failed (non-core, skipped): {exc}")
                continue
            n_vals = 0
            for blk in blocks:
                code_str = str(blk.get('code', ''))[:6]
                if not code_str.isdigit():
                    continue
                try:
                    record_date = date(int(code_str[:4]), int(code_str[4:6]), 1)
                except ValueError:
                    continue
                for v in blk.get('values', []):
                    hit = iid_map.get(v.get('_id'))
                    if hit is None:
                        continue
                    code, field = hit
                    val = self._to_float(v.get('value'))
                    if val is None:
                        continue
                    if not self._retail_value_ok(field, val):
                        self.logger.warning(
                            f"  Retail out-of-range dropped: "
                            f"{code}.{field}@{record_date} = {val}")
                        continue
                    cells.setdefault((record_date, code), {})[field] = val
                    n_vals += 1
            self.logger.info(f"  Retail leaf {cid[:8]}: {n_vals} values")
            self.rate_limit_pause(0.5)

        if 0 < core_fail < len(core_cids):
            raise RuntimeError(
                f"Retail: {core_fail}/{len(core_cids)} core leaves failed; "
                f"refusing partial write")
        if core_fail == len(core_cids):
            if cells:
                raise RuntimeError(
                    "Retail: all core leaves failed but non-core succeeded; "
                    "inconsistent API state")
            self.logger.warning(
                "  Retail v2 API unavailable; relying on official releases")

        api_max_date = max((d for (d, c) in cells if c == 'total'), default=None)
        total_months = len({d for (d, c) in cells if c == 'total'})
        if full:
            if not cells:
                raise RuntimeError(
                    "Retail: initial/full fetch requires the v2 API; "
                    "refusing to seed from releases only")
            if total_months < 400:
                raise RuntimeError(
                    f"Retail full fetch only got {total_months} months for "
                    f"'total' (expected ~500+); refusing to write")

        # -- 新闻稿：常态补最新期 + API 不可用时灾备 --------------------------
        # Revisit recent releases even when a later month exists in the API.
        # Otherwise a missed June ('上半年') can never be repaired after July.
        anchor = api_max_date or latest_date
        release_after = anchor - timedelta(days=370) if anchor else None
        try:
            # 回看发布列表，补齐曾被标题解析漏掉的月份（例如「上半年」）。
            release_cells = self._fetch_retail_releases(
                after_date=release_after, max_pages=8)
        except Exception as exc:
            if not cells:
                raise
            self.logger.warning(f"  Retail release fallback failed: {exc}")
            release_cells = {}
        added = 0
        for key, fields in release_cells.items():
            tgt = cells.setdefault(key, {})
            for field, val in fields.items():
                if tgt.get(field) is None:
                    tgt[field] = val
                    added += 1
        if added:
            self.logger.info(f"  Retail: {added} cells added from official releases")
        if not cells:
            raise RuntimeError("No retail rows fetched")
        if api_max_date is None and release_cells:
            self._last_fetch_partial = True

        rows = []
        for (record_date, code), fields in sorted(cells.items()):
            vals = [fields.get(f) for f in RETAIL_FIELDS]
            if all(v is None for v in vals):
                continue
            rows.append((record_date, code, code_names.get(code, code), *vals))
        return rows

    # -- Main entry ---------------------------------------------------------

    def fetch_series(self, series_config):
        """Fetch and store one NBS series."""
        table = series_config['table']
        func_name = series_config['fetch_func']
        fetch_func = getattr(self, func_name)

        conn = self.get_connection()
        try:
            self.ensure_table(conn, series_config['create_sql'])
            self._last_fetch_partial = False

            # Incremental: LAST13 if table has data, LAST180 if empty
            # (NBS_HOUSE_FULL=1 强制全量 2011-01 起重灌，用于历史数据修复)
            if func_name == '_fetch_house_price':
                latest = self.get_latest_date(conn, table)
                if latest and os.environ.get('NBS_HOUSE_FULL') != '1':
                    period = 'LAST13'
                    self.logger.info(f"  Fetching {table} (incremental LAST13, latest: {latest})...")
                else:
                    period = 'LAST180'
                    self.logger.info(f"  Fetching {table} (full LAST180 = 2011-01+)...")
                rows = fetch_func(period=period, latest_date=latest)
            elif func_name in ('_fetch_real_estate_macro', '_fetch_retail_sales'):
                latest = self.get_latest_date(conn, table)
                self.logger.info(f"  Fetching {table} (latest: {latest})...")
                rows = fetch_func(latest_date=latest)
            else:
                self.logger.info(f"  Fetching {table}...")
                rows = fetch_func()
            self.logger.info(f"  Got {len(rows)} rows for {table}")

            if series_config.get('strategy') == 'truncate' and not self._last_fetch_partial:
                self.truncate_and_insert(
                    conn, table, series_config['columns'], rows
                )
            else:
                self.upsert_rows(
                    conn, table, series_config['columns'], rows,
                    on_duplicate_update=series_config.get('update_columns'),
                    coalesce=series_config.get('coalesce_update', False),
                )
            self.rate_limit_pause(1.0)
        finally:
            if conn.is_connected():
                conn.close()
