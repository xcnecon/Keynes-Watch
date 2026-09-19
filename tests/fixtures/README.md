# Public source fixtures

Retrieved on 2026-09-07. These are deliberately curated public test inputs,
not production database exports. HTML keeps the source table text, element
nesting, whitespace, and row/column spans; unrelated styles are removed.

- `mts_202607.json`: [Treasury MTS Table 1, report dated 2026-07-31](https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/mts/mts_table_1?filter=record_date:eq:2026-07-31). All 26 source rows, projected onto the fetcher's fields. Both fiscal-year headings and their parent/child IDs are preserved.
- `omo_no_operation.html`: [PBOC notice 2026/173, September 4](https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125431/125475/2026090408480534617/index.html). The operation has zero allotment and no rate column.
- `omo_operation.html`: [PBOC notice 2026/174, September 7](https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125431/125475/2026090709060799588/index.html). The notice states 1.40% and CNY 500 million; HTML splits the header and decimal with whitespace.
- `retail_june.html`: [NBS June / first-half 2026 retail sales release](https://www.stats.gov.cn/sj/zxfb/202607/t20260715_1964127.html). Only the first indicator table is retained. June total is CNY 4,269.1 billion; first-half total is CNY 24,872.2 billion, stored in source units of CNY 100 million.

Other small inline test values are synthetic regression cases.
