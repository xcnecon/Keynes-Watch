# Keynes Watch 逐页数据审计 — 2026-09-07

基线：`main@98adc2e5a2ace8f10e521a107627a6b373a85661`。

检查了两个入口和全部 32 个站内数据页面的默认图表，包括服务端生成的
Plotly 数据、单位、日期、筛选项和对应抓取程序；对异常页进一步查询官方
响应，复现计算。额外检查了 Repo/Reverse Repo 同时显示和美国三部门余额
请求 2026Q2 的结果。不是对每种筛选组合、每个历史观测值的逐点认证。

仓库只有 ETL，没有网页路由、模板、查询代码或生产数据库。本 PR 修复
可确认的 ETL 问题并提供回填行为；未连接或修改生产数据库，未部署网页。

## 已确认并修复

### 1. MTS 把本财年和上财年同名月份相加

原 `fields` 没有 `classification_id` / `parent_id`。财政部 API 会对省略
维度后的相同键自动汇总数值；`record_calendar_year` 是报告年份，不能区分
报告中列出的两财年。这个行为见 [Fiscal Data API 文档](https://fiscaldata.treasury.gov/api-documentation/)。

用原字段请求 2026-07-31 报告，July 赤字为 **723.45053490915 billion USD**；
完整源记录中的 FY2025 July 为 **291.14266028767 billion**、FY2026 July 为
**432.30787462148 billion**，两者之和恰好等于原结果。
线上 2026 曲线从 June 到 July 的增量也是这个错误的两年合计。

修复保留行身份，根据 `parent_id → FY YYYY` 给月份定年，正确处理十月至
十二月跨财年。每个观察月取最新报告版本，排除 YTD 合计，校验
`outlays - receipts = deficit`；缺失收入或支出直接失败，不补零。
所有可用报告正常回读并 upsert，既修复旧错值也接受修订，保留 API 覆盖前
已有历史。财政部当前 137 份报告、3,090 行经过校验，得到 154 个月，范围
2013-10 至 2026-07。

[完整原始报告 API](https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/mts/mts_table_1?filter=record_date:eq:2026-07-31)

注意：现网页画的是 **1 月开始的日历年累计**，不是 10 月开始的美国财政年
累计。此展示约定没有在 ETL 中改动；网页如要显示 FYTD，需要网页代码。

### 2. 中国利率走廊把零操作量当成 0% 利率

2026-09-02 至 09-04 等零操作日的公告表只有“期限、投标量、中标量”。
原解析器把第三列当成旧格式的“中标利率”，因此写入 0%。
另有公告把“操作 利率”和“1. 40 %”拆成含空白的 HTML 文本。

修复按列名识别利率和中标量，清理字符间空白。零操作日保存 `rate=NULL`、
`volume=0`；有操作的 2026-09-07 公告解析为 7 天、1.40%、5 亿元。
官方数据更新改为 upsert，并把回看起点延伸至已有的最早错误零利率，纠正
已有记录。网页需要保留 NULL，或在单独的政策利率曲线上沿用最后一次公告
利率，不能把 NULL 补成 0。

[9 月 4 日零操作公告](https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125431/125475/2026090408480534617/index.html)；
[9 月 7 日公告](https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125431/125475/2026090709060799588/index.html)

### 3. 中国社零漏掉 2026 年 6 月

线上 2026 曲线从 5 月跳到 7 月。官方六月公告标题为“2026年上半年社会
消费品零售总额增长1.3%”，原程序只接受数字月份标题。
修复识别上半年、一季度、前三季度、全年；发布稿回看不再以最新 API 月份
为硬截止，因此七月已存在时仍可补六月。

实际六月表成功解析 25 个指标，总量为：当月 **42,691 亿元**、累计
**248,722 亿元**；当月同比 1.0%、累计同比 1.3%。月度与累计分别保存。

[NBS 原始六月报告](https://www.stats.gov.cn/sj/zxfb/202607/t20260715_1964127.html)

### 4. 国债平均剩余期限重复纳入汇总行

原查询用 `maturity IS NOT NULL` 判断明细。第一次计算后，分类汇总和
Total Marketable 行也获得 `maturity`，后续更新把它们重新计权；分类权重
也可能赋给汇总行。

修复只让具有实际、未到期 `maturity_date` 的债券明细参与计算，并清理
汇总行权重。即使没有新月份也重新计算，能够修复已存汇总。
用 2026-07-31 官方 887 行源数据重算，Total Marketable 为 **5.840237 年**；
线上为 **5.87 年**。在隔离 SQL 数据库中重复运行，修复后的结果保持不变。

[MSPD Table 3 原始 API](https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/debt/mspd/mspd_table_3_market?filter=record_date:eq:2026-07-31)

### 5. Repo 与 Reverse Repo 共用日期水位，掩盖单个序列停更

线上 Repo 到 2026-09-04，Reverse Repo 停在 2026-07-02；官方 API 的
09-04 Reverse Repo 有 **675 million USD** 的结果。原程序用整张表的
最大日期过滤，会跳过较慢序列缺失的所有日期。

修复分别读取两种操作的最新日期，从较早者回看七天并 upsert。只处理
`auctionStatus=Results`，按日累加实际不同操作，排除重复 operation ID，
不会把尚未出结果的公告金额缺失当作零。

[NY Fed 原始 API](https://markets.newyorkfed.org/api/rp/results/search.json?startDate=2026-07-01)；
[官方操作页面](https://www.newyorkfed.org/markets/desk-operations/repo)

### 6. GDP / CPI / 准备金的旧观测不接收修订

这些单序列原来只抓 `MAX(date)+1` 之后的值，因此 GDP 的第二、第三次估计
以及历史修订不会更新已有日期。相关 CPI/GDP 调整和准备金占比可能使用
陈旧数据。改为全历史 upsert，与现有就业等表采用同样策略。
这项由代码和修订机制确认，未取得生产数据库来量化每个分母的偏差。

[BEA GDP 发布与估计](https://www.bea.gov/data/gdp/gross-domestic-product)

## 逐页结果

“未见明显异常”仅表示此次默认图表、口径与抓取逻辑检查未确认错误；
不代表所有历史值与筛选组合均已逐点核实。

| 页面 | 结果 |
| --- | --- |
| `/us/mts` | 确认跨财年合计错误；已修复并支持回填。日历年累计标签见上文。 |
| `/us/tga_balance` | 2026-09-03 closing = 903.928bn，与源行相符；未误用 opening。 |
| `/us/treasuries_outstanding` | 默认选择 Amount Held by Public，不能直接与总债务比较；未见明显错值。GDP/CPI 修订问题已修复。 |
| `/us/treasuries_average_interest_rates` | 默认 Total Marketable 口径，未见明显异常。 |
| `/us/average_treasury_maturities` | 确认汇总递归计权；已修复。 |
| `/us/debt_limit` | 未见明显异常。 |
| `/us/reserve` | 默认准备金量级与单位未见明显异常；修订更新问题已修复。 |
| `/us/repo` | 确认 Reverse Repo 缺失最近两个月、Repo 已到最新；水位逻辑已修复。 |
| `/us/stir` | 默认 EFFR 和目标区间量级正常，但输出含重复日期，详见下方限制。 |
| `/us/yield_curve` | 默认最新名义曲线可读，单位为 %，未见明显异常。 |
| `/us/withheld_tax` | 年度累计和源单位转换未见明显异常。 |
| `/us/payrolls` | 单位为千人，图为当年累计增量；未见明显异常。 |
| `/us/claims` | 年间叠图为每周 claims 水平，并非求和；未见明显错值。 |
| `/us/ur` | U3/U6 百分比未见明显异常。 |
| `/us/indeed` | 美国工资同比，未见明显异常。 |
| `/us/indeed_job_post` | 招聘指数，未见明显异常。 |
| `/us/kalecki_equation` | 最新至 2026Q2，分项正负合计约为零；未见明显数值异常。 |
| `/us/three_sector_balance` | 私人净贷出官方最新为 2026Q1，因此日期本身正常；GDP–GDI discrepancy 被额外堆叠的问题属于网页层。 |
| `/cn/lpr` | 未见明显异常。 |
| `/cn/money_supply` | 官方同比及 M1-M2 差值未见明显异常；不应用口径变化后的绝对量自算跨年增速。 |
| `/cn/social_financing` | 默认累计增量口径未见明显异常。 |
| `/cn/new_credit` | 7 月负增量本身不是错误，累计贷款流可下降；未据此改写数据。 |
| `/cn/rrr` | 图为政策调整历史，最后调整日期不等于每日数据停更；未见明确错值。 |
| `/cn/shibor` | 确认零操作量误读为零利率；已修复。 |
| `/cn/balance_sheet` | 默认资产分项单位为亿元，未见明显异常。 |
| `/cn/real_estate_macro` | 后续网页代码核验发现先截日期再差分、把1—2月累计当作2月单月值的问题；需在网页层修复。 |
| `/cn/house_price` | 指数基准为 100，不是同比百分比；未见明显异常。 |
| `/cn/real_estate_climate` | 历史序列止于 2025-12，主页明确标记 historical；不是最新月度覆盖。 |
| `/cn/land_revenue` | 累计财政收入，未见明显数值异常；2023 覆盖不完整，不能当完整月度历史。 |
| `/cn/retail_sales` | 确认缺 2026-06；已修复标题解析和回看补数。 |
| `/cn/kalecki_equation` | 年度分项加总约为零，未见明显数值异常。 |
| `/cn/three_sector_balance` | 年度分项含统计误差加总约为零，未见明显数值异常。 |

## 仓库缺少网页代码，尚未修复的展示问题

1. **美国三部门余额**：2026Q1 的三条部门序列已合计为 0，却额外堆叠
   GDP–GDI discrepancy。后续网页核查已定位到直接将该列加入部门柱形。
   **更正日期判断**：官方 [私人净贷出 W994RC1Q027SBEA](https://fred.stlouisfed.org/series/W994RC1Q027SBEA)
   最新也是 2026Q1；[NIPA 5.1 发布表](https://fred.stlouisfed.org/release/tables?eid=7581&rid=53)
   的 Q2 私人净贷出为空，因此不能因 Kalecki 已有 Q2 就认定三部门停更。
2. **美国短端利率**：默认 EFFR trace 有 1,097 个点但只有 753 个不同日期，
   重复日期值相同。后续网页核查确认读取整表，没有 join；当前建表声明
   `(record_date,type)` 主键。未读取生产 schema，不能断言旧表缺少哪项约束。
   展示端需要消除相同重复点，对冲突值留空；本 ETL 仓库不包含该路由。
3. **生产是否恢复**：此 PR 没有连接生产数据库、执行更新脚本或清除网页缓存。
   修复需在生产 updater 使用新代码后运行相应 source；网页层残留需另行修复。

## 验证

- 离线回归覆盖 MTS 年份、修订、符号、单位和无效源数据；平均期限重复更新；
  Repo 序列落后、每日多次操作和未完成公告；PBOC 零操作公告和格式变化；
  社零半年标题和已有七月时补六月；FRED 同日期修订。
- 财政部完整 MTS API 历史实际解析及收入/支出/赤字恒等式校验通过。
- 使用真实 MSPD 明细进行隔离 SQL 重算，重复运行数值不变。
- 真实 PBOC 六份公告、NBS 六月原表解析通过。
- 无生产 MySQL 连接；数据库相关测试使用隔离替身和 SQLite 执行实际筛选谓词。
