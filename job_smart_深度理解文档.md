# Job-Smart 项目深度理解文档

> 用途：面试 Analytics Engineer / Data Engineer 时，能如实、自信地讲解这个项目并应对追问。
> 原则：本文档只基于 repo 里**真实存在**的代码。凡是代码里没做的，会明确标注「这里没做」，绝不美化。
> 读法：每个技术点用四段式——**概念是什么 / 你项目里怎么实现 / 面试怎么答 / 高危追问**。

---

## 第 0 章：一句话讲清这个项目

> 面试开场白模板（背下来）：
>
> "这是一个端到端的求职市场数据平台。它每天通过 GitHub Actions 定时从三个来源——Adzuna、JSearch 两个招聘 API 和一批公司的 Greenhouse/Ashby 招聘门户——抓取美国科技岗位，加载到 Snowflake，再用 dbt 做分层建模（staging 清洗 + marts 聚合），最后用 Streamlit 做成一个有薪资、技能、AI 趋势分析的看板，还接了一个基于 LLM 的简历匹配分析功能。"

这段话覆盖了：数据源、编排、仓库、转换、展示、AI——面试官听完就知道你做了一条完整的数据链路。后面每一章就是把这句话拆开讲透。

---

## 第 1 章：全局架构 —— 数据从哪来，到哪去

### 概念：什么是 ELT，为什么是 ELT 不是 ETL

传统 **ETL** 是 Extract（抽取）→ Transform（转换）→ Load（加载）：数据在进仓库**之前**就transform好。
现代 **ELT** 是 Extract → Load → Transform：先把原始数据**原封不动**灌进数据仓库（Snowflake），**在仓库里**用 SQL 做转换。

为什么现代数据栈都转向 ELT：
- 云数仓（Snowflake/BigQuery）算力极强且便宜，在仓库里跑 SQL 转换比在外面用 Python 跑更快、更易维护。
- 原始数据先留底（RAW 层），转换逻辑错了可以重跑，不丢数据。
- 转换用 SQL（dbt），版本可控、可测试、团队可协作。

**你的项目就是标准 ELT**：fetch 脚本只做最轻量的抽取和加载（E+L），所有清洗、合并、聚合都在 Snowflake 里用 dbt 完成（T）。

### 你项目里的真实数据流

```
[数据源 - 3个 live 源]
  ├─ Adzuna API        (scripts/fetch_adzuna.py)   —— 9个角色关键词，每个最多500条
  ├─ JSearch API       (scripts/fetch_jsearch.py)  —— Indeed+LinkedIn 聚合，req/月有限
  └─ Greenhouse/Ashby  (scripts/fetch_portals.py)  —— 目标公司官方招聘API，无需key
        │
        │  (E+L: Python 抓取 → 直接写入)
        ▼
[Snowflake RAW schema]   —— 原始数据留底
  ├─ RAW.ADZUNA_POSTINGS
  ├─ RAW.JSEARCH_POSTINGS
  └─ RAW.PORTAL_POSTINGS
        │
        │  (T: dbt staging — 清洗、标准化、字段对齐)
        ▼
[dbt STAGING schema]  (物化为 view)
  ├─ stg_adzuna_postings    ┐
  ├─ stg_jsearch_postings   ├─→ stg_all_postings  (三源 UNION + 跨源去重)
  └─ stg_portal_postings    ┘
        │
        │  (T: dbt marts — 聚合成分析主题)
        ▼
[dbt MARTS schema]  (物化为 table) —— 11张表
  薪资类: mart_salary_by_role / _by_seniority / _by_role_seniority / _by_company_tier / mart_jobs_by_location
  技能类: mart_skill_demand / mart_tech_skills / mart_tech_skills_by_role / mart_ai_skill_cooccurrence
  趋势类: mart_hiring_trends / mart_remote_trend
        │
        │  (展示)
        ▼
[Streamlit Dashboard]  (dashboard/app.py, 4个页面)
  Market Overview / Compensation / Skills Intelligence / Resume Analyzer(接 Groq LLM)
```

### 面试怎么答

被问"讲讲你这个项目的架构"，照着上面那张图从上往下说：源 → RAW → staging → marts → dashboard，并强调两个设计选择：(1) ELT 而非 ETL，原始数据留底在 RAW；(2) dbt 分层，staging 负责"清洗对齐"、marts 负责"业务聚合"，职责分离。

### 高危追问

**Q: 为什么分 staging 和 marts 两层，不直接从 RAW 建最终表？**
A: 分层是为了**复用和可维护**。staging 层把三个源清洗成统一格式（同样的列名、同样的类型、同样的 salary 口径），这一层的成果被下游 11 张 mart 共享——如果不分层，每张 mart 都要重复写一遍清洗逻辑。staging 改一次，所有 mart 自动受益。这是 dbt 的核心范式（staging → intermediate → marts）。

**Q: 你这个 RAW 层有没有做留底/历史归档？**
A: （诚实版）RAW 表是每天 append 新数据。fetch_adzuna 里有一段 `DELETE FROM ... WHERE FETCHED_AT LIKE '今天%'`，作用是同一天重跑时先删掉今天已抓的、避免重复，但**历史数据是累积保留的**。我没有做正式的快照/SCD 历史维度——这是可以改进的点。

---

## 第 2 章：编排层 —— Airflow vs GitHub Actions（最重要，必被问）

> ⚠️ 这是你整个项目最容易被问倒、也最容易讲错的地方。务必搞清楚。

### 事实：你有两套编排，但只有一套天天在跑

**真正每天在跑的是 GitHub Actions**，不是 Airflow。

| | GitHub Actions | Airflow DAG |
|---|---|---|
| 文件 | `.github/workflows/fetch_adzuna.yml` | `dags/job_market_pipeline.py` |
| 定时 | `cron: '0 22 * * *'` (UTC 22:00 = LA 15:00) | `schedule_interval='0 6 * * *'` |
| 运行环境 | GitHub 免费托管，自动触发 | 需本地 `docker compose up` 把容器跑起来 |
| 实际状态 | **真在跑**（截图里 37 次 run、~4min/次） | 本地实现/演示用，非线上常驻 |

### 概念：什么是编排（orchestration），为什么需要它

一条 pipeline 有很多步骤（抓数→加载→转换→测试），它们有**依赖顺序**（必须先抓到数据才能加载，先加载才能转换），还需要**定时触发**、**失败重试**、**失败告警**。手动一步步跑不现实，所以需要"编排工具"来自动按顺序、按时间调度这些任务。

- **GitHub Actions**：本质是 CI/CD 工具，靠 cron 定时触发，按 yml 里写的 step 顺序执行。轻量、免费、不用自己维护服务器。
- **Apache Airflow**：专业的数据编排工具，用 DAG（有向无环图）定义任务依赖，有 Web UI、重试机制、任务级监控。是数据工程行业标准，但要自己部署维护。

### 你项目里两套各自怎么实现的

**GitHub Actions（`fetch_adzuna.yml`，真跑的那个）** 的执行顺序：
1. checkout 代码 → 装 Python 依赖
2. 把 Snowflake 私钥写入临时文件（密钥认证）
3. 跑 `fetch_adzuna.py`（抓 Adzuna）
4. 跑 `fetch_jsearch.py`（抓 JSearch）
5. 跑 `fetch_portals.py`（抓 Greenhouse/Ashby）
6. `dbt run`（建模）
7. `dbt test`（数据质量测试）
8. 跑 healthcare 扫描 + 邮件摘要
9. 在 GitHub run summary 里输出每个源插入了多少行

**Airflow DAG（`job_market_pipeline.py`，本地实现的那个）** 定义的任务依赖：
```python
download_data >> upload_to_snowflake >> dbt_run >> dbt_test
```
配了 `retries: 2`、`retry_delay: 5分钟`——这就是编排工具相比裸脚本的价值：自动重试。

### 面试怎么答（诚实且加分的标准答法）

被问"你用什么编排的"：

> "线上每天实际运行的是 GitHub Actions——用 cron 每天 LA 时间下午3点触发，按顺序跑三个源的抓取、然后 dbt run 和 dbt test，端到端大概4分钟。我同时也用 Apache Airflow 实现了同一条 pipeline，把它建成一个 DAG，用 `download >> upload >> dbt_run >> dbt_test` 定义任务依赖，配了失败重试——这一版是容器化在本地用 Docker Compose 跑的，主要是为了实践 Airflow 这个行业标准编排工具。"

这样答的好处：诚实（不假装 Airflow 天天在跑）、且展示你**两套都懂**、还知道它们的适用场景区别。

### 高危追问

**Q: 既然 GitHub Actions 够用，为什么还要 Airflow？/ 它俩区别是什么？**
A: GitHub Actions 适合轻量、低频的定时任务，免费且零运维，所以我线上用它。但 Airflow 在真实数据团队里是标准——它有任务级的 DAG 依赖管理、Web UI 可视化每个任务状态、更强的重试和回填（backfill）能力。我做 Airflow 版是为了学这套行业标准，理解 DAG、operator、task dependency 这些概念。

**Q: 你的 GitHub Actions 如果某一步失败了会怎样？**
A: （看代码诚实答）后面的步骤会受影响。我在最后一步用了 `if: always()` 来保证无论成败都输出一个 run summary。但我**没有**配置专门的失败告警（比如失败发邮件），这是可以加强的——Airflow 那版倒是配了 retry。

**Q: cron 表达式 `0 22 * * *` 什么意思？**
A: 分钟0、小时22、每天、每月、每周不限——即每天 UTC 22:00 触发。换算成洛杉矶夏令时（PDT, UTC-7）就是下午 3 点。

**Q: 为什么 Airflow 的 schedule 是 6am 而 GitHub Actions 是 22:00？它俩不冲突吗？**
A: （诚实）它俩是独立的两套，时间是分别设的，没刻意对齐。线上以 GitHub Actions 为准；Airflow 那版是本地按需启动演示的，schedule 只是 DAG 定义里的默认值。

---

## 第 3 章：抽取层 —— 三个数据源怎么抓的

### 3.1 Adzuna (`fetch_adzuna.py`)

**怎么抓**：对 9 个角色关键词（data engineer / data scientist / data analyst / ML engineer / ai engineer / software engineer / analytics engineer / business intelligence / llm engineer）逐个查询，每个角色翻 10 页、每页 50 条，理论上每角色最多 500 条。每次请求之间 `time.sleep(0.5)` 避免触发限流。

**抓完怎么处理**：
- `parse_job()` 把 Adzuna 的 JSON 拍平成统一字段（JOB_ID, TITLE, COMPANY, SALARY_MIN/MAX/EST, STATE, CITY...）。
- salary 估算：`SALARY_EST = (salary_min + salary_max) / 2`。
- **第一次去重**：`df.drop_duplicates(subset=["JOB_ID"])`——同一次抓取里，相同 job id 只留一条（因为不同角色关键词可能搜到同一个岗位）。

**怎么写 Snowflake**：用 `write_pandas`（snowflake-connector 提供的批量写入），写前先 `DELETE` 掉今天已抓的数据，避免同天重跑产生重复。

### 3.2 JSearch (`fetch_jsearch.py`)

聚合了 Indeed + LinkedIn 的数据（通过 RapidAPI）。**关键细节**：它在抓取阶段就做了 **salary 年化**（annualize）——因为不同岗位标的薪资周期不同（时薪/月薪/年薪）：
```python
def annualize(v):
    if period in ("HOUR", "HOURLY"):  return round(v * 2080, 0)   # 2080 = 40小时 × 52周
    if period in ("MONTH", "MONTHLY"): return round(v * 12, 0)
    # 否则当作年薪
```
> ⚠️ 注意：JSearch 的月限额很小（代码注释写 200 req/月），所以 `PAGES_PER_QUERY` 设得很低。这是面试可以提的"在 API 限额约束下做的工程取舍"。

### 3.3 Greenhouse / Ashby Portals (`fetch_portals.py`)

**这是最能体现工程巧思的一个源**。Greenhouse 和 Ashby 是招聘系统（ATS），很多公司用它们管招聘。它们提供**公开 API、无需 key**：
- Greenhouse: `https://boards-api.greenhouse.io/v1/boards/{公司slug}/jobs`
- Ashby: `https://api.ashbyhq.com/posting-api/job-board/{公司slug}`

**标题过滤**：抓回来的岗位用 `title_matches()` 做正负向关键词过滤——正向词（data analyst, analytics engineer, data engineer, healthcare data...）保留，负向词（senior, staff, principal, director, ios, android, blockchain...）排除。这样只留下相关的、初中级的岗位。

**JOB_ID 设计**：greenhouse 的加前缀 `gh_`，ashby 的加 `ashby_`，避免两个系统的 id 撞车。

> ⚠️ portal 源**没有薪资数据**（SALARY 全是 None）——因为公司官方 JD 通常不写薪资。这点在后面去重逻辑里很关键。

### 面试怎么答

"我有三个互补的数据源：Adzuna 和 JSearch 是商业招聘 API，覆盖面广、带薪资；Greenhouse/Ashby 是公司官方 ATS 的公开接口，数据最权威、无需 key，但不带薪资。三个源各有取舍，合起来既有广度又有薪资信号。"

### 高危追问

**Q: 三个源的数据格式不一样，你怎么统一的？**
A: 在抓取层各自的 `parse_job` 先拍平成大致统一的字段写进 RAW；真正的字段对齐和类型统一是在 dbt staging 层做的（见第4章）——每个源一个 stg 模型，输出完全相同的 schema，再 UNION。

**Q: Adzuna 那个去重 `drop_duplicates(subset=["JOB_ID"])` 为什么需要？**
A: 因为我用 9 个角色关键词分别搜索，同一个岗位可能同时匹配多个关键词（比如一个岗位既匹配 "data engineer" 又匹配 "analytics engineer"），导致重复抓取。按 JOB_ID 去重保证每个岗位在单次抓取里只留一条。

---

（接下页：第4章 staging 清洗、第5章 跨源去重、第6章 marts、第7章 数据质量、第8章 AI功能、第9章 短板、第10章 面试题库）

---

## 第 4 章：Staging 清洗层 —— 把三个源对齐成一种格式

### 概念：staging 层在 dbt 里干什么

staging 是 dbt 的第一层转换，职责单一：**对每个数据源做"一对一"的清洗**——重命名列、转换类型、做轻量标准化，但**不做跨表 join、不做聚合**。原则是"一个源一个 stg 模型，输出干净、统一、可信的明细数据"。

你的 staging 物化为 **view**（`dbt_project.yml` 里 `staging: +materialized: view`）——view 不占存储、每次查询实时计算，适合这种轻量清洗层。（对比：marts 物化为 table，因为聚合计算重、需要固化结果。）

### 你三个 stg 模型做了什么（都输出相同 schema）

`stg_adzuna_postings` / `stg_jsearch_postings` / `stg_portal_postings` 输出的列完全一致：job_id, company_name, job_title, description, location, state_code, min/max/med_salary, pay_period, work_type, remote_allowed, experience_level, listed_at, annual_salary_est, source, _loaded_at...

几个关键的标准化技巧（面试金句）：

**1. work_type 用正则从描述里推断**：
```sql
case
  when lower(DESCRIPTION) rlike '.*(contract|contractor|freelance|1099|c2c).*' then 'CONTRACT'
  when lower(DESCRIPTION) rlike '.*(part[ -]time).*'                          then 'PART_TIME'
  else 'FULL_TIME'
end as work_type
```

**2. experience_level 用正则从 job_title 推断**（标准化成 EN/MI/SE/DI/EX 五级）：
```sql
case
  when lower(TITLE) rlike '.*(vp |vice president|director|head of|chief).*' then 'EX'
  when lower(TITLE) rlike '.*(staff |principal |lead ).*'                    then 'DI'
  when lower(TITLE) rlike '.*(senior|sr).*'                                  then 'SE'
  when lower(TITLE) rlike '.*(junior|jr|entry.level|associate).*'            then 'EN'
  else 'MI'
end as experience_level
```
> 注释里写了 "infer from title (more reliable than description)"——你做过判断：从标题推断比从描述推断更准。这是面试可讲的细节。

**3. salary 口径统一**：三个源最后都输出 `annual_salary_est`（年薪估算）。Adzuna 在 SQL 里直接用 SALARY_EST；JSearch 在抓取时已年化；portal 没薪资所以是 null。

**4. remote_allowed**：综合 location 和 description 判断，分 '1'(remote) / 'hybrid' / '0'。

### 面试怎么答

"staging 层我每个源建一个模型，做三件事：重命名成统一列名、转换类型、轻量标准化。最花心思的是把三个源的字段对齐成完全相同的 schema，这样下游才能 UNION。比如 experience level，我用正则从职位标题里抽取，归一成 EN/MI/SE/DI/EX 五个等级；salary 统一成年薪口径。"

### 高危追问

**Q: 用正则从 title/description 推断 seniority 和 work_type，准确率怎么样？有什么局限？**
A: （诚实）这是基于规则的启发式，不是 100% 准。局限是：标题不规范的会落到默认值（MI / FULL_TIME），可能有误判。但对于做宏观趋势分析（看大盘分布）来说足够了；如果要做精确的单条判断，就需要更复杂的 NLP。这是准确率和工程成本的权衡。

**Q: 为什么 staging 用 view，marts 用 table？**
A: view 不占存储、实时计算，适合 staging 这种轻量清洗、且作为中间层频繁被引用的场景。marts 是聚合结果、计算较重、要被 dashboard 反复查询，固化成 table 查询更快。这是 dbt 里典型的物化策略选择。

---

## 第 5 章：跨源去重 —— 你项目最漂亮的一段 SQL（重点准备）

> 这段是 `stg_all_postings.sql` 的核心，是你简历里 "cross-source deduplication via SQL window functions" 的出处。面试官如果懂行，一定会问。务必讲到能手写。

### 问题：三个源会抓到同一个岗位

同一个岗位，可能 Adzuna 有、JSearch 也有、公司官网也有。直接 UNION 三个源会产生重复。但难点是：**它们的 JOB_ID 完全不同**（各家系统自己的 id），没法用 id 去重。

### 解法：用业务键去重 + 窗口函数选最优

```sql
unioned as (
    select * from adzuna
    union all
    select * from jsearch
    union all
    select * from portals
),
deduped as (
    select *,
        row_number() over (
            partition by
                lower(trim(company_name)),    -- 业务键1: 公司名(标准化)
                lower(trim(job_title)),        -- 业务键2: 职位名(标准化)
                date(listed_at)                -- 业务键3: 发布日期
            order by
                case when annual_salary_est is not null then 0 else 1 end,  -- 有薪资的排前面
                source                                                       -- 再按源名排序(决定性)
        ) as rn
    from unioned
)
select * exclude(rn) from deduped where rn = 1
```

### 概念：window function + row_number 去重模式

这是数据工程里最经典的去重模式，逐层拆解：

1. **partition by（分组键）**：把"同一个岗位"定义为 `公司名 + 职位名 + 发布日期` 三者都相同。用 `lower(trim())` 标准化，避免大小写/空格导致的漏判。这就是"业务键去重"——当没有可靠的技术主键时，用业务含义上的"同一性"来去重。

2. **row_number() over (... order by ...)**：在每个分组内给每行编号 1,2,3...。编号顺序由 order by 决定。

3. **order by 的精妙之处**：
   - 第一排序键 `case when annual_salary_est is not null then 0 else 1`：**有薪资的记录排第一**。因为 portal 源没薪资，这样去重后会优先保留 Adzuna/JSearch 那条带薪资的，信息更全。
   - 第二排序键 `source`：当薪资情况相同时，按源名字母序决定，保证结果**确定性**（每次跑结果一样，不随机）。

4. **where rn = 1**：每个分组只留编号为 1 的那条——即"最优的那条"。

5. `select * exclude(rn)`：Snowflake 语法，输出时排除掉辅助列 rn。

### 面试怎么答（这段要能脱口而出）

"三个源会抓到同一个岗位，但它们的 ID 是各系统私有的、对不上。所以我用**业务键去重**：把'公司名+职位名+发布日期'作为同一性的判断，用 `row_number()` 窗口函数在每组内编号。关键在 order by——我让有薪资的记录排在前面，这样去重时优先保留信息更全的那条（因为公司官网那个源没薪资）；再用 source 做次级排序保证结果确定性。最后取每组 row_number = 1 的。"

### 高危追问

**Q: 为什么用 row_number 而不是 GROUP BY 或 DISTINCT？**
A: GROUP BY/DISTINCT 只能去重、没法"选择保留哪一条"。我需要在重复记录里**挑信息最全的那条**（有薪资的），这需要排序后取第一条，正是 row_number + order by 的能力。DISTINCT 做不到这种"带优先级的去重"。

**Q: 用'公司+职位+日期'做业务键，会不会误杀？比如同一家公司同一天发了两个同名但不同的岗位？**
A: 有这个理论风险，是个 trade-off。但实际中同公司同天发完全同名的不同岗位很少；而跨源重复（同一岗位被多个平台收录）非常常见。所以这个业务键在"防误杀"和"去重复"之间是合理的权衡。如果要更严格，可以加上 location 或 job description 的相似度，但会增加复杂度。

**Q: row_number、rank、dense_rank 区别？**
A: row_number 给连续唯一编号（1,2,3,4），即使值相同也不并列；rank 遇到相同值会并列且跳号（1,1,3）；dense_rank 并列但不跳号（1,1,2）。去重要用 row_number，因为我要每组**恰好**保留一条，不能并列。

---

## 第 6 章：Marts 层 —— 11 张表，三大类

### 概念：mart 是什么

mart（数据集market/集市）是面向**具体分析主题**的聚合表，直接给 BI/dashboard 用。每张 mart 回答一个业务问题。你的 11 张分三类：

**薪资类（5张）**
- `mart_salary_by_role`：按角色聚类算薪资分布（含 median, p10/p25/p75/p90 分位数）
- `mart_salary_by_seniority`：按资历等级
- `mart_salary_by_role_seniority`：角色×资历交叉
- `mart_salary_by_company_tier`：按公司分层（FAANG+/Scale-up/Enterprise/Startup）
- `mart_jobs_by_location`：按地理位置

**技能类（4张）**
- `mart_skill_demand`：技能需求排名（多少岗位要这个技能）
- `mart_tech_skills` / `mart_tech_skills_by_role`：技术技能（整体 / 按角色）
- `mart_ai_skill_cooccurrence`：AI/LLM 岗位里哪些技能一起出现

**趋势类（2张）**
- `mart_hiring_trends`：按月的招聘量趋势
- `mart_remote_trend`：远程趋势

### 重点讲两张（最可能被问）

**`mart_salary_by_role` —— 角色聚类 + 分位数**

先把五花八门的 job_title 用 `case when like` 归并成标准角色（Data Engineer / Data Scientist / ML Engineer...），再 group by 算各角色的薪资统计。亮点是用了 `percentile_cont(0.25/0.75) within group (order by ...)` 算分位数——比只看平均值更能反映真实分布（平均值会被极端高薪拉偏，中位数+分位数更稳健）。还做了脏数据过滤：`where annual_salary_est > 10000 and < 1000000`。

**`mart_salary_by_company_tier` —— 公司分层**

用一大段 `rlike` 正则把公司名归类：匹配 google/meta/amazon/openai/anthropic... → FAANG+；匹配 stripe/databricks/figma... → Scale-up；匹配 oracle/sap/salesforce... → Enterprise；其余有效公司名 → Startup/Other。然后按 tier 算薪资中位数，最后用 case 给 tier 排序（FAANG+ → Scale-up → Enterprise → Startup）。

**`mart_ai_skill_cooccurrence` —— AI 技能共现（你简历里的亮点）**

逻辑分三步：
1. 定义一个技能词表（python, spark, langchain, pytorch... 50+ 个），用 `flatten(parse_json(...))` 拆成行。
2. 筛出"AI/LLM 岗位"：description 里提到 llm/generative ai/gpt/rag/embedding... 或 title 是 ai/ml engineer 的。
3. 对每个技能，数它在多少 AI 岗位的描述里出现（`cross join` + `like`），算占比和排名。
=> 回答"做 AI 的岗位最常要求哪些技能"。

### 面试怎么答

"marts 层是11张面向分析主题的聚合表，分薪资、技能、趋势三类。设计上我注意了两点：一是薪资用中位数和分位数而不是只看平均，因为薪资分布右偏、平均会被高薪拉偏；二是做了脏数据过滤，比如薪资明显异常的（<1万或>100万）剔除。"

### 高危追问

**Q: 公司分层用硬编码的公司名正则，扩展性是不是很差？**
A: （诚实）对，这是个明显的局限。新公司不在列表里就会落到"Startup/Other"。更好的做法是维护一张公司维度表（dim_companies）、或接公司信息API。我现在这版是为了快速出分析结果用的硬编码，是已知的可改进点。

**Q: 平均薪资和中位数你为什么都算了，看哪个？**
A: 主要看中位数。薪资分布是右偏的（少数极高薪拉高平均），中位数更能代表"典型"水平。我两个都算是为了在 dashboard 上能对比、也能看出偏度。

---

## 第 7 章：数据质量 —— dbt tests

### 概念：dbt test 是什么

dbt 内置数据质量测试。在 schema.yml 里给某列声明 `tests: [not_null, unique]`，dbt 跑 `dbt test` 时会自动生成 SQL 去检查这一列有没有空值/重复，有就报错。这是"数据质量左移"——在数据进入下游前就拦住问题。

### 你项目里测了什么

staging 层（`models/staging/schema.yml`）：
- `stg_job_postings.job_id`: not_null + unique
- `stg_companies.company_id`: not_null + unique
- 多个关键字段 not_null

marts 层（`models/marts/schema.yml`）：
- `mart_skill_demand.skill_id`: not_null + unique
- `mart_salary_by_role.role_cluster`: not_null + unique

而且声明了 dbt **sources**（raw 层 6 张表），这是 dbt 管理数据血缘（lineage）的正规做法。

CI 里（GitHub Actions）跑完 `dbt run` 紧接着 `dbt test`——测试失败会在 run 里体现。

### 面试怎么答

"我用 dbt 的 schema test 做数据质量保障，对关键主键列加了 not_null 和 unique 测试，比如 job_id、company_id、skill_id。每天 pipeline 在 dbt run 之后会自动跑 dbt test，相当于把数据质量检查嵌进了日常流程。"

### 高危追问

**Q: 你的 dbt test 如果失败了，pipeline 会停吗？数据会不会带着问题进 dashboard？**
A: （诚实）目前 dbt test 是在 dbt run **之后**跑的，所以即使 test 失败，模型其实已经建好了。更严格的做法是用 `dbt build`（它会按依赖顺序 run+test，一个模型 test 失败就不会build依赖它的下游），或者把 test 设成阻断式。这是可以改进的。

**Q: 除了 not_null/unique，dbt 还能测什么？你为什么没用更多？**
A: dbt 还有 accepted_values（值域）、relationships（外键关系）等内置测试，也能写自定义测试。我目前主要覆盖了主键完整性，更全面的测试（比如 salary 值域、experience_level 的 accepted_values）是可以加的。

---

## 第 8 章：AI 功能 —— Resume Analyzer（诚实归因，别答错）

> ⚠️ 这里最容易把自己讲进坑。务必诚实。

### 事实

Resume Analyzer（dashboard 第4页）是真实能用的：贴简历+JD，输出 match score、matched/critical gaps/nice-to-have、learning path、tailoring tips。

它的实现（`ai/resume_analyzer.py`）：**调用 Groq 托管的 Llama 3.3 70B 模型**（`MODEL = "llama-3.3-70b-versatile"`），用 LLM 的 structured JSON output 功能（`response_format={"type": "json_object"}`）来抽取结构化信息。三个函数：extract_skills_from_resume / extract_skills_from_jd / skill_gap_analysis。

> ⚠️ 还有一个 `generate_learning_path`（90天路线图）功能，代码里有、但线上点了出不来结果（疑似 bug）。**所以简历里没写它，面试也别主动提它能用。** 如果被问到，就说"那个功能我实现了但还在调试，没上线"。

### 面试怎么答（诚实归因）

"Resume Analyzer 是我接了一个 LLM 做的——用 Groq 托管的 Llama 3.3 70B，通过它的 structured JSON output 功能，把简历和职位描述抽取成结构化的技能画像，再做匹配度和技能差距分析。**模型不是我训练的，我做的是 prompt 设计、JSON schema 定义、和把它集成进 dashboard 的工程部分。**"

最后那句是关键——划清"我用了别人的模型"和"我训练了模型"的界限。绝对不要让面试官以为你做了 NLP 模型训练。

### 高危追问

**Q: 这个 AI 分析是你自己做的模型吗？**
A: 不是。我用的是 Groq 平台托管的开源 Llama 3.3 70B。我的工作是：设计 prompt 让它按我定义的 JSON 结构输出、处理返回结果、集成到 Streamlit。本质是 LLM 应用集成，不是模型训练。

**Q: 为什么选 Groq 不选 OpenAI？**
A: Groq 有免费额度（不要信用卡）、推理速度快、且 API 兼容 OpenAI 格式。对一个个人项目来说，免费和易用是主要考量。

**Q: 你怎么保证 LLM 返回的是合法 JSON？**
A: 用了 Groq/OpenAI 的 `response_format={"type": "json_object"}` 参数，强制模型输出合法 JSON，再用 `json.loads` 解析。这比让模型自由输出再正则提取可靠得多。

---

## 第 9 章：项目真实短板清单（你要心里有数，可能被问）

诚实面对这些，面试时被问到能从容答"这是已知可改进点"，比被问倒强一百倍：

1. **没有增量加载**：staging 是全量 view，每次重算全部数据。数据量大了会慢。改进方向：dbt incremental models。
2. **dbt test 在 run 之后跑**：test 失败不阻断模型构建。改进：用 `dbt build` 或阻断式测试。
3. **公司分层靠硬编码正则**：新公司会落到 Other，扩展性差。改进：维护公司维度表。
4. **seniority/work_type 靠正则启发式**：不规范标题会误判。改进：更强的 NLP 或人工规则迭代。
5. **没有失败告警**：GitHub Actions 失败不会主动通知。改进：加 Slack/邮件告警。
6. **learning path 功能有 bug**：线上点不出来，未修复。
7. **RAW 层无正式历史快照/SCD**：只是累积 append。
8. **Airflow 版非常驻**：需手动启动，不是真正的生产编排。

### 面试怎么答这类问题

被问"这个项目有什么不足/如果重做你会怎么改"——这是高频题，照上面挑2-3个答，体现你有工程判断力。比如："最想改的是加增量加载，现在 staging 全量重算，数据量上去会有性能问题；另外想把 dbt test 改成阻断式，让数据质量问题能真正卡住 pipeline。"

---

## 第 10 章：面试高危问题题库（20题速记）

> 每题配一句话标准答法的核心。面试前过一遍。

**架构类**
1. 讲讲整体架构 → 三源→RAW→staging→marts→dashboard，ELT 模式，dbt 分层。
2. 为什么 ELT 不是 ETL → 原始数据留底、在仓库里用 SQL 转换、可重跑可测试。
3. 为什么分 staging/marts → 职责分离，staging 清洗对齐、marts 聚合，staging 成果被多个 mart 复用。

**编排类**
4. 用什么编排的 → 线上 GitHub Actions（cron 每天3pm，~4min），另用 Airflow 实现了 DAG 版本。
5. Airflow 和 GitHub Actions 区别 → GHA 轻量免费零运维；Airflow 是行业标准，有 DAG 依赖/UI/重试。
6. cron '0 22 * * *' → 每天 UTC22:00 = LA 15:00。
7. pipeline 失败怎么办 → GHA 有 always 输出 summary，Airflow 配了 retry；没做失败告警(待改进)。

**数据源类**
8. 三个源什么区别 → Adzuna/JSearch 商业API带薪资覆盖广；portal 是公司ATS官方接口最权威但无薪资。
9. 数据格式不同怎么统一 → staging 每源一个模型输出相同 schema 再 UNION。

**SQL/去重类（重点）**
10. 跨源怎么去重 → 业务键(公司+职位+日期)去重，row_number 窗口函数，order by 让有薪资的优先保留。
11. 为什么 row_number 不用 DISTINCT → 我要带优先级地"选保留哪条"，DISTINCT 做不到。
12. row_number/rank/dense_rank 区别 → 见第5章。
13. 业务键会不会误杀 → 有 trade-off，但跨源重复远多于同名不同岗，权衡合理。

**建模类**
14. salary 为什么用中位数+分位数 → 薪资右偏，平均被高薪拉偏，中位数更稳健。
15. 公司分层怎么做的 → rlike 正则匹配公司名分 FAANG+/Scale-up/Enterprise/Startup（硬编码,待改进）。
16. AI 技能共现怎么算 → 技能词表 × AI岗位描述做 like 匹配计数排名。
17. staging 为什么 view、marts 为什么 table → view 轻量实时适合清洗层；table 固化聚合结果查询快。

**数据质量类**
18. 怎么保证数据质量 → dbt schema test，主键 not_null+unique，CI 里跑 dbt test。
19. test 失败会怎样 → 目前 run 后跑 test 不阻断(待改进为 dbt build)。

**AI类**
20. AI 分析是你训的模型吗 → 不是，调 Groq 的 Llama 3.3 70B，我做 prompt 设计+JSON schema+集成，是 LLM 应用不是模型训练。

---

## 附：最该背熟的三段话

1. **开场架构**（第0章那段）
2. **编排诚实版**（第2章："线上跑 GitHub Actions...同时用 Airflow 实现了 DAG..."）
3. **去重逻辑**（第5章："业务键去重，row_number，order by 让有薪资的优先..."）

把这三段讲顺，这个项目的面试就稳住一大半了。

---

*文档基于 job-smart repo 真实代码编写。如代码更新，相应章节需同步。*
