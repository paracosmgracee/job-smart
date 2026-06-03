# Job-Smart 项目深度学习文档
## 面向 Analytics Engineer / Data Engineer 岗位面试

> 本文档基于 repo 中真实存在的代码编写。所有文件名、函数名、SQL 字段均引用实际代码。
> 如果某个最佳实践在本项目中**没有实现**，会明确标注「本项目未实现」。

---

## 一、项目全景

这是一个端到端的职位市场情报系统，每天自动从三个外部 API 抓取职位数据，写入 Snowflake，通过 dbt 做多层转换，最终在 Streamlit dashboard 上展示分析结果。

**数据流：**
```
[Adzuna API] ──┐
[JSearch API] ─┼──→ GitHub Actions (每日 UTC 22:00) ──→ Snowflake RAW ──→ dbt staging ──→ dbt marts ──→ Streamlit
[ATS Portals] ─┘
(Greenhouse / Ashby)
```

**关键技术栈：**
- 抓取：Python（requests, pandas, snowflake-connector-python）
- 认证：RSA key-pair（非密码，绕过 MFA）
- 编排：GitHub Actions（`.github/workflows/fetch_adzuna.yml`）
- 数仓：Snowflake（database: JOB_MARKET）
- 转换：dbt（project: job_market）
- 展示：Streamlit（Streamlit Cloud 托管）

---

## 二、数据源层

### 2.1 Adzuna（`scripts/fetch_adzuna.py`）

**概念：** Adzuna 是一个招聘聚合平台，提供付费 REST API。需要注册获取 `APP_ID` 和 `APP_KEY`。

**实现：**
- 入口：`main()` 函数循环 9 个关键词（`"data engineer"`, `"data scientist"`, `"data analyst"`, `"machine learning engineer"`, `"ai engineer"`, `"software engineer"`, `"analytics engineer"`, `"business intelligence"`, `"llm engineer"`）
- 每个关键词最多取 10 页，每页 50 条，即最多 500 条/关键词
- 请求参数：`sort_by=date`, `max_days_old=30`（只取近 30 天数据）
- 薪资字段：Adzuna 直接返回年薪（`salary_min`, `salary_max`），脚本计算 `SALARY_EST = (min + max) / 2`
- 状态字段：`REMOTE` 字段在 Adzuna 数据里为 None（Adzuna API 不提供该字段），后续在 dbt staging 层从 description 里推断
- 写入：`write_pandas(conn, df, "ADZUNA_POSTINGS", overwrite=False)`
- 去重保护：写入前先 `DELETE WHERE FETCHED_AT LIKE '{today}%'`，防止当天重跑产生重复

**面试追问准备：**
- Q: 你们抓 Adzuna 数据有没有考虑 rate limit？
  A: 有。每页请求后 `time.sleep(0.5)` 做了基础限速。Adzuna 免费 tier 有月请求上限，每天 9 个词 × 10 页 = 90 次请求，远低于上限。
- Q: 如果同一个职位被多个关键词检索到怎么处理？
  A: `df.drop_duplicates(subset=["JOB_ID"])` 在内存里先去重，只写一条到 Snowflake。

---

### 2.2 JSearch（`scripts/fetch_jsearch.py`）

**概念：** JSearch 是 RapidAPI 上的第三方服务，聚合了 Indeed + LinkedIn 的职位数据。按月计费，免费 tier 每月 200 次请求。

**实现：**
- 循环 8 个关键词，每个词取 3 页（`PAGES_PER_QUERY = 3`）
- 请求量：8 × 3 = 24 次/天，留有 buffer 应对重跑
- **薪资年化处理**（在 Python 里做，不是 dbt）：
  ```python
  def annualize(v):
      if period == "HOUR":  return round(v * 2080, 0)   # 52周×40小时
      if period in ("WEEK", "WEEKLY"):  return round(v * 52, 0)
      if period in ("MONTH", "MONTHLY"):  return round(v * 12, 0)
      return v  # 已经是年薪
  ```
  写入 Snowflake 的 `SALARY_MIN`, `SALARY_MAX`, `SALARY_EST` 都已经是年薪。

- `REMOTE` 字段：直接用 API 返回的 `job_is_remote` 布尔值，存为 `"1"` 或 `"0"`

**面试追问准备：**
- Q: 为什么薪资年化在 Python 做而不是在 dbt 做？
  A: 因为 `pay_period` 字段在 RAW 层没有单独存，丢了就没法年化。所以在抓取时先做，保证写进 Snowflake 的已经是统一口径的年薪。这算个设计决策——trade-off 是 Python 里的业务逻辑更难测试和追踪。
- Q: Indeed/LinkedIn 数据的可靠性怎么样？
  A: JSearch 是第三方聚合，数据质量取决于他们的爬取频率。薪资字段缺失率较高，很多职位不披露薪资。

---

### 2.3 ATS Portals（`scripts/fetch_portals.py`）

**概念：** Greenhouse 和 Ashby 是企业常用的 ATS（Applicant Tracking System，招聘管理系统）。两者都提供完全公开的 job board API，不需要任何 API key。

**实现：**
- 公司列表来自 `portals.yml`（50+ 家目标公司），通过正则解析提取 ATS 类型和 slug
- Greenhouse API：`GET https://boards-api.greenhouse.io/v1/boards/{slug}/jobs`
- Ashby API：`GET https://api.ashbyhq.com/posting-api/job-board/{slug}`
- 在 Python 里做 title filter：只保留 title 包含 `data analyst`/`data engineer`/`data scientist` 等关键词的职位，过滤掉 `senior`/`director`/`manager` 等
- **Portal 数据没有薪资**：Greenhouse/Ashby 的公开 API 不返回薪资信息，所有薪资字段写为 null

**面试追问准备：**
- Q: ATS 数据的 JOB_ID 格式是什么？会不会和其他源冲突？
  A: 代码里加了前缀：Greenhouse 的 ID 存为 `"gh_{id}"`，Ashby 的存为 `"ashby_{id}"`。JSearch 的 ID 来自原始 `job_id` 字段，Adzuna 用数字 ID。三个源的 ID 格式本来就不同，加前缀进一步保证唯一性。

---

## 三、编排层（重要：这里有容易被问倒的地方）

### 3.1 实际运行的编排：GitHub Actions

**文件：** `.github/workflows/fetch_adzuna.yml`

**概念：** GitHub Actions 是 GitHub 内置的 CI/CD 工具，可以按 cron 触发 workflow。不需要额外部署任何服务。

**实现：**
- 触发：`schedule: cron: '0 22 * * *'`（每天 UTC 22:00，即 LA 时间下午 3 点）
- 任务顺序（顺序执行，串行）：
  1. `python scripts/fetch_adzuna.py` → 写 `RAW.ADZUNA_POSTINGS`
  2. `python scripts/fetch_jsearch.py` → 写 `RAW.JSEARCH_POSTINGS`
  3. `python scripts/fetch_portals.py` → 写 `RAW.PORTAL_POSTINGS`
  4. `dbt run --select stg_portal_postings stg_jsearch_postings stg_adzuna_postings stg_all_postings+`
  5. `dbt test --select stg_adzuna_postings stg_jsearch_postings`
  6. `python scripts/fetch_healthcare.py`（email digest，独立功能）
- 认证：私钥通过 `secrets.SNOWFLAKE_PRIVATE_KEY` 注入，workflow 里先 `echo "$SNOWFLAKE_PRIVATE_KEY" > /tmp/snowflake_key.pem`，dbt 使用这个文件

### 3.2 Airflow DAG（本项目未真正运行）

**文件：** `dags/job_market_pipeline.py`

**诚实说明（面试时必须知道）：** 这个文件是早期原型，**实际上没有在运行**。可以从以下细节看出：
1. DAG 的 description 里写着 `"ETL: Kaggle/Adzuna → Snowflake"`，任务 1 还在调用 `download_data.py`（Kaggle 下载脚本）
2. BashOperator 里的路径是 `/opt/airflow/scripts/`，这是 Airflow 的 Docker 部署路径，本地 repo 没有配置 Airflow 环境
3. 整个 pipeline 现在由 GitHub Actions 负责，DAG 文件没有被更新成反映当前架构的版本

**面试怎么回应：**
> "DAG 文件是项目早期设计 Airflow 编排时留下的原型，后来因为这个项目规模比较小、数据量不大，最终选择用 GitHub Actions 代替，成本更低且不需要额外维护 Airflow 基础设施。现在实际跑的是 GitHub Actions，DAG 文件没有删掉但也没有同步更新，这是个技术债。如果要生产化，Airflow 或 Prefect 会是更合适的选择，因为它们提供更好的可观测性和任务依赖管理。"

---

## 四、Snowflake RAW 层

**概念：** RAW 层是数据仓库的第一落地点，存放从外部系统直接摄入的数据，不做业务逻辑转换，只做格式存储。目的是保留原始数据方便 debug 和重跑。

**实现：**

RAW schema 下有两类表：

**① 活跃表（每天被写入）：**
| 表名 | 数据来源 | 特点 |
|------|---------|------|
| `ADZUNA_POSTINGS` | Adzuna API | 有 SALARY_MIN/MAX/EST，无 REMOTE 字段 |
| `JSEARCH_POSTINGS` | JSearch/RapidAPI | 薪资已年化，有 REMOTE 字段（"1"/"0"） |
| `PORTAL_POSTINGS` | Greenhouse/Ashby | 无薪资，有 REMOTE 字段，有过滤后的 title |

**② 静态表（来自早期 Kaggle 数据集，不再更新）：**
`JOB_POSTINGS`, `JOB_SKILLS`, `JOB_SALARIES`, `JOB_INDUSTRIES`, `COMPANIES`
这些是通过 `scripts/upload_to_snowflake.py` 一次性上传的 LinkedIn Kaggle 数据集，**没有进入最终 dashboard**（dashboard 只读 MARTS 层，marts 都从 `stg_all_postings` 聚合，而 `stg_all_postings` 只 union 三个活跃表）。

**写入模式（delete-before-insert，非 upsert）：**
```python
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
cur.execute(f"DELETE FROM RAW.ADZUNA_POSTINGS WHERE FETCHED_AT LIKE '{today}%'")
write_pandas(conn, df, "ADZUNA_POSTINGS", overwrite=False)
```
这不是真正的 incremental load，更接近"每日全量快照"。好处是实现简单，坏处是：
1. 历史数据会随时间积累（不做分区的话表会持续增大）
2. 如果同一个职位 30 天内持续挂着，每天都会存一条新的 FETCHED_AT 不同的记录

---

## 五、dbt Staging 层

**概念：** Staging 层是数仓的第一个转换层。核心职责：字段重命名/类型转换、数据清洗、派生字段（从原始字段推断新字段）、多源合并。Staging 模型通常 materialized 为 view（不存储数据，每次查询时实时计算）。

**本项目配置（`dbt_project/dbt_project.yml`）：**
```yaml
models:
  job_market:
    staging:
      +schema: STAGING
      +materialized: view    # 所有 staging 模型都是 view
    marts:
      +schema: MARTS
      +materialized: table   # 所有 mart 模型都是物理表
```

**关于 `generate_schema_name` 宏（`dbt_project/macros/generate_schema_name.sql`）：**
dbt 默认会把 schema 拼接为 `{target.schema}_{custom_schema_name}`（比如 `STAGING_STAGING`），这个宏把它改成直接用 `custom_schema_name`（大写），所以 staging 模型写进 `STAGING` schema，marts 写进 `MARTS` schema。

### 5.1 三个单源 staging 模型

**`stg_adzuna_postings.sql`、`stg_jsearch_postings.sql`、`stg_portal_postings.sql`**

三个模型结构类似，都做以下事情：

**字段标准化（每个源字段名不同，统一成相同字段名）：**
```sql
JOB_ID::varchar          as job_id,
trim(COMPANY)            as company_name,
trim(TITLE)              as job_title,
SALARY_MIN::float        as min_salary,
try_to_timestamp(POSTED_AT) as listed_at
```

**`work_type` 推断（从 description 里用 regex）：**
```sql
case
    when lower(DESCRIPTION) rlike '.*(contract|contractor|freelance|1099|corp.to.corp|c2c).*' then 'CONTRACT'
    when lower(DESCRIPTION) rlike '.*(part[ -]time).*'                                        then 'PART_TIME'
    else 'FULL_TIME'
end as work_type
```

**`experience_level` 推断（从 job title）：**
```sql
case
    when lower(TITLE) rlike '.*(vp |vice president|director|head of|chief).*' then 'EX'
    when lower(TITLE) rlike '.*(staff |principal |lead ).*'                    then 'DI'
    when lower(TITLE) rlike '.*(senior|sr\.? |sr-).*'                          then 'SE'
    when lower(TITLE) rlike '.*(junior|jr\.? |jr-|entry.level|associate).*'   then 'EN'
    else 'MI'
end as experience_level
```
注意：默认是 `'MI'`（Mid Level）。大量职位 title 里没有级别词，都会被归为 MI，这会使 MI 数量虚高。

**`remote_allowed` 处理：**
- Adzuna：从 LOCATION 和 DESCRIPTION 两个地方推断，结果为 `'1'` / `'hybrid'` / `'0'`
- JSearch：直接用 API 返回的布尔字段 `REMOTE`，`coalesce(REMOTE, '0')`
- Portal：从 REMOTE 字段（"1"/"0"）和 LOCATION 里的 "remote" 关键词推断

**注意：** 三个源的 `remote_allowed` 编码**不完全一致**——Adzuna 能返回 `'hybrid'`，JSearch 只有 `'1'`/`'0'`，Portal 只有 `'1'`/`'0'`。这个不一致在后续聚合时有影响（见 Bug 章节）。

**Portal 源没有薪资数据：**
```sql
null::float   as min_salary,
null::float   as max_salary,
null::float   as med_salary,
null::float   as annual_salary_est,
```
所有来自 Greenhouse/Ashby 的目标公司职位在薪资分析中都会被过滤掉（因为 salary IS NULL）。

### 5.2 跨源合并去重：`stg_all_postings.sql`

**这是整个 staging 层最核心的模型，面试必须能流利解释。**

```sql
-- Step 1: UNION ALL 三个 staging 模型
unioned as (
    select * from adzuna
    union all
    select * from jsearch
    union all
    select * from portals
),

-- Step 2: 用 ROW_NUMBER 去重
deduped as (
    select *,
        row_number() over (
            partition by
                lower(trim(company_name)),   -- 公司名（小写去空格）
                lower(trim(job_title)),      -- 职位名（小写去空格）
                date(listed_at)             -- 发布日期（精确到天）
            order by
                -- 优先保留有薪资数据的那条
                case when annual_salary_est is not null then 0 else 1 end,
                source   -- 薪资相同时按 source 排序（adzuna < ashby < greenhouse < jsearch）
        ) as rn
    from unioned
)

select * exclude(rn)
from deduped
where rn = 1
```

**去重逻辑解释：**
- 同一个职位可能被多个源同时抓到（比如一个 Stripe 的 DA 岗位，Adzuna 抓到了，JSearch 也抓到了）
- 用 `PARTITION BY company + title + date` 把"相同职位"归组
- `ORDER BY salary IS NOT NULL` 保留有薪资数据的那条——因为 JSearch 比 Portal 更可能有薪资
- 这不是完美去重，因为同一个职位可能在不同源里 title 写法略有差异

**本项目未实现：** 没有基于 description 的相似度去重（只做了精确字符串匹配）。

**面试追问准备：**
- Q: 为什么用 `UNION ALL` 而不是 `UNION`？
  A: `UNION` 会做全行比较去重，性能更差，而且不同源的字段值不一样（比如 source 字段），所以 `UNION` 去不掉跨源重复。我们用 `UNION ALL` 先合并，再用 `ROW_NUMBER` 按业务逻辑去重，逻辑更可控。
- Q: `ROW_NUMBER` 和 `RANK` 有什么区别，这里为什么用 `ROW_NUMBER`？
  A: `RANK` 在 tie 情况下会给相同的排名，导致多行都是 rank=1，去不掉重复。`ROW_NUMBER` 保证每行唯一，哪怕完全相同的两行也会有不同的 row number，所以 `where rn = 1` 一定只保留一行。

---

## 六、dbt Marts 层

**概念：** Marts 层是数仓的最终分析层，存放面向具体业务问题的聚合结果。materialized 为 table（物理存储），让 BI 工具直接查询时不需要每次从头计算。

**本项目所有 mart 表都从 `stg_all_postings` 出发**（除了 `mart_skill_demand`，见下面的特殊说明）。

### 6.1 薪资分析类（4 张表）

**`mart_salary_by_role.sql`**
- 把 job_title 用 LIKE 归成 11 个 role cluster（Data Engineer、Data Scientist、Data Analyst 等）
- 对每个 cluster 计算 avg、median、p10、p25、p75、p90
- 过滤条件：`annual_salary_est BETWEEN 10000 AND 1000000`（过滤明显错误数据）
- 输出：每个 role cluster 一行，包含薪资分布的各分位数

**`mart_salary_by_seniority.sql`**
- 直接用 staging 层推断好的 `experience_level` 字段（EN/MI/SE/EX/DI）
- 计算每个级别的 p10、median、p90
- 过滤：`annual_salary_est BETWEEN 20000 AND 800000`

**`mart_salary_by_role_seniority.sql`**
- Role cluster × Seniority 的交叉聚合（用于 grouped bar chart）
- 输出：(role_cluster, seniority) 组合的 median 薪资

**`mart_salary_by_company_tier.sql`**
- 用正则对 company_name 做 tier 分类：FAANG+、Scale-up、Enterprise、Startup/Other
- FAANG+ 包含：Google、Meta、Amazon、Apple、Netflix、Microsoft、OpenAI、Anthropic、NVIDIA 等
- Scale-up 包含：Stripe、Databricks、Snowflake、Airbnb、DoorDash 等
- **分类方法局限**：纯 string matching，拼写不一致的公司名（如 "Meta Platforms"）可能匹配不到

### 6.2 技能分析类（4 张表）

**`mart_tech_skills.sql` 和 `mart_tech_skills_by_role.sql`**

**核心方法：hardcoded keyword list × cross join：**
```sql
with tech_skills as (
    select trim(value::string) as skill
    from table(flatten(input => parse_json('[
        "python","sql","spark","aws","dbt","snowflake","tableau",...
    ]')))
),
matches as (
    select s.skill, j.job_id
    from tech_skills s
    cross join jobs j
    where j.desc_lower like '%' || s.skill || '%'   -- ILIKE substring match
)
```

**本项目未实现：** 没有 NLP 实体识别、没有 skill ontology（比如把 "PySpark" 和 "Apache Spark" 归为同一个技能）。

**局限性（面试时可以主动说）：**
- False positives：比如 "r studio" 可能在某些 description 里匹配到不相关的词
- 没覆盖到的技能不会出现在结果里，是个封闭集合

**`mart_ai_skill_cooccurrence.sql`**
- 先筛选出"AI 相关"职位（description 里提到 LLM、RAG、GPT 等关键词）
- 再看这些 AI 职位里最常要求哪些技术技能
- 体现了"哪些技能跟 AI 岗位绑定在一起"的洞察

**`mart_skill_demand.sql`（特殊！使用的是 Kaggle 数据）**
- 这个模型 references `stg_job_skills`，而 `stg_job_skills` 来自 `RAW.JOB_SKILLS`——这是 Kaggle 静态数据集的 skill tag 表
- **这意味着 `mart_skill_demand` 反映的是 Kaggle 数据集里的技能分布，不是实时数据**
- 这是个隐藏的数据口径问题

### 6.3 地理分析（1 张表）

**`mart_jobs_by_location.sql`**

这里解决了一个关键的多源数据标准化问题：Adzuna 返回全称州名（"California"），JSearch 返回两字母代码（"CA"）。

```sql
case trim(state_code)
    when 'California' then 'CA'
    when 'New York'   then 'NY'
    -- ... 50 个州全部枚举
    else upper(trim(state_code))  -- 已经是两字母的直接通过
end as state_code_2
```

过滤：`length(state_code_2) = 2` 过滤掉格式不对的（比如 "United States"、"Remote"）

**本项目未实现：** 这个 state 标准化只在这一张 mart 做了，其他直接用 `state_code` 字段的地方仍然是混合格式。

### 6.4 趋势类（2 张表）

**`mart_hiring_trends.sql`**
- 用 `date_trunc('month', listed_at)` 按月统计各 role cluster 的 posting 数量
- 体现招聘热度随时间的变化

**`mart_remote_trend.sql`（这里有 Bug）**
见下一章节。

---

## 七、已知 Bug 和技术短板（面试前必读）

### Bug 1：`mart_remote_trend.sql` 字段值不匹配

```sql
-- 代码里这样写：
count(case when remote_allowed = 'TRUE'  then 1 end) as remote_count,
count(case when remote_allowed = 'FALSE' then 1 end) as onsite_count,
count(case when remote_allowed is null   then 1 end) as hybrid_count

-- 但 staging 层实际存的值是：
-- '1', '0', 'hybrid' (不是 'TRUE'/'FALSE')
```

**后果：** `remote_count` 和 `onsite_count` 永远是 0，`hybrid_count` 也永远是 0（因为值不为 null，只是不是 `'TRUE'`/`'FALSE'`）。所有行都进了 `total - remote - onsite - hybrid` 的"哑"分类。这张表的数字是错的。

**面试怎么回应：**
> "我发现了这个 bug，是因为 staging 层存的是 `'1'`/`'0'`/`'hybrid'`，但 mart 里 hardcode 了 `'TRUE'`/`'FALSE'`，两边对不上。修复方式是把 mart 里的条件改成 `remote_allowed = '1'` 和 `remote_allowed = '0'`。这种问题在多人协作或者分阶段开发时比较容易发生，正确做法是在 staging 层就统一好枚举值，并在 schema.yml 里加 `accepted_values` test。"

### Bug/限制 2：`mart_skill_demand` 用的是 Kaggle 静态数据

已在上文说明。如果面试官问"skill demand 这张表的数据多新"，回答"这张表实际上引用的是一次性上传的 LinkedIn Kaggle 数据集，不是实时数据"。

### 技术短板 3：没有真正的增量加载（Incremental Load）

**什么是增量加载：** 每次只处理新增或变更的数据，而不是每次全量重跑。好处是省时省 compute，坏处是实现更复杂（需要 watermark 或 CDC）。

**本项目实现：** delete-before-insert，每天把当天的数据删掉重写。这是**全量日快照**，不是增量。优点：实现简单，幂等（重跑不产生重复）。缺点：如果数据量增长，每天 DELETE+INSERT 的开销会变大。

**面试怎么回应：**
> "目前是 delete-before-insert 的全量日快照模式。数据量不大（每天几千行）所以可以接受。如果要做增量，可以在 dbt 里把模型改成 `materialized='incremental'`，配合 `unique_key` 做 merge，或者在 fetch 脚本里加 `WHERE updated_at > last_run_time` 的 watermark 逻辑。"

### 技术短板 4：dbt 测试覆盖率低

`dbt_project/models/staging/schema.yml` 里只定义了三个模型的测试：
- `stg_job_postings`（Kaggle 数据）：`job_id` 的 not_null + unique，`company_name` 的 not_null
- `stg_job_skills`：`job_id` + `skill_id` 的 not_null
- `stg_companies`：`company_id` 的 not_null + unique

**三个实时 staging 模型（`stg_adzuna_postings`, `stg_jsearch_postings`, `stg_portal_postings`）和 `stg_all_postings` 都没有 dbt 测试。**

marts 的 `schema.yml` 只有 `mart_skill_demand` 和 `mart_salary_by_role` 的少量 column tests。

**面试怎么回应：**
> "测试覆盖是明显不足的地方。正确做法是在每个 staging 模型上至少加 `job_id` 的 not_null + unique 测试，在 `stg_all_postings` 上加 source freshness check（比如确保每天确实有新数据写入），以及在薪资字段上加 range 测试（比如 `annual_salary_est < 0` 的记录不应该存在）。现在 GitHub Actions 里的 `dbt test` 步骤实际上只测了很少的模型。"

### 技术短板 5：没有 SCD（Slowly Changing Dimensions）

这个项目所有的表都是快照模式，没有追踪职位状态变化（比如一个职位昨天在，今天下架了）。**本项目未实现**。

### 技术短板 6：没有数据监控/Alerting

没有配置 dbt source freshness 告警，没有 row count 异常检测，没有 Slack/email 通知。GitHub Actions step summary 会打印插入行数，但没有自动 alert 阈值。**本项目未实现**。

### 技术短板 7：Airflow DAG 是过时的原型

`dags/job_market_pipeline.py` 存在但不运行，且内容与现有架构不符（还在引用 Kaggle 数据）。这是个技术债。

---

## 八、认证机制（RSA Key-Pair Auth）

**背景：** Snowflake 从 trial 转为 paid account 后默认开启 MFA（多因素认证），密码登录会失败。解决方案是改用 RSA key-pair 认证，它不触发 MFA。

**生成私钥流程（本地已完成）：**
```bash
openssl genrsa -out ~/.ssh/snowflake_key.pem 2048
openssl rsa -in ~/.ssh/snowflake_key.pem -pubout -out ~/.ssh/snowflake_key_pub.pem
# 把公钥注册到 Snowflake 用户
ALTER USER PARACOSMGRACEE SET RSA_PUBLIC_KEY='MIIBIjANBgkq...';
```

**Python 里的使用（三个 fetch 脚本和 `dashboard/app.py` 都一样）：**
```python
from cryptography.hazmat.primitives import serialization

def get_conn():
    pk = serialization.load_pem_private_key(
        os.environ["SNOWFLAKE_PRIVATE_KEY"].encode(), password=None
    )
    pk_bytes = pk.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=..., user=..., private_key=pk_bytes, ...
    )
```

`SNOWFLAKE_PRIVATE_KEY` 是 PEM 格式的字符串（包含 BEGIN/END RSA PRIVATE KEY 头尾），通过 GitHub Actions Secrets 和 Streamlit Cloud Secrets 注入。

**dbt 用法：** `dbt_project/profiles.yml` 里指定 `private_key_path: /tmp/snowflake_key.pem`，GitHub Actions 先把 secret 写到这个文件路径。

---

## 九、Streamlit Dashboard 层

**文件：** `dashboard/app.py`

**连接 Snowflake：** 用 `@st.cache_resource` 缓存连接（避免每次 rerun 重新建连），用 `@st.cache_data(ttl=3600)` 缓存查询结果（1 小时内不重复查 Snowflake）。

```python
@st.cache_resource
def _new_conn():
    pk = serialization.load_pem_private_key(
        _secret("SNOWFLAKE_PRIVATE_KEY").encode(), password=None
    )
    ...

@st.cache_data(ttl=3600)
def load_data(query: str) -> pd.DataFrame:
    with _new_conn() as conn:
        return pd.read_sql(query, conn)
```

**Secrets 读取：** 本地用 `.streamlit/secrets.toml`（gitignore 的），Streamlit Cloud 里在界面上配置。

**Dashboard 只读 MARTS 层**，不直接查 RAW 或 STAGING（这是正确的架构）。

---

## 十、面试高危问题清单

### 数据抓取 & 编排

**Q1: 你的 pipeline 是怎么触发的？用了什么编排工具？**

A: 用 GitHub Actions，文件是 `.github/workflows/fetch_adzuna.yml`，每天 UTC 22:00 通过 cron 触发。三个 fetch 脚本顺序执行，最后跑 dbt run 和 dbt test。repo 里有一个 Airflow DAG 文件（`dags/job_market_pipeline.py`），但那是早期原型，实际没有运行。选择 GitHub Actions 是因为这个项目规模较小，不需要 Airflow 的复杂调度能力，GitHub Actions 无需额外部署基础设施，成本更低。

---

**Q2: 如果某一天 Adzuna API 挂了，pipeline 怎么处理？**

A: `fetch_adzuna.py` 里对每一页的请求做了 try/except，单页失败会打印错误并 break 出内层循环，继续处理下一个 keyword。整个脚本失败会导致 GitHub Actions step 失败，但因为 workflow 是顺序执行的（不是 fail-fast=false），后续的 fetch_jsearch 和 fetch_portals 会继续跑。当天的 ADZUNA_POSTINGS 可能只有部分数据或者没有数据，但其他两个源不受影响。没有配置重试或告警通知，这是个改进点。

---

**Q3: 你的数据是增量加载还是全量加载？**

A: 目前是 delete-before-insert 模式，每天先删当天的数据再重新插入。严格来说这是全量日快照，不是真正的增量加载。好处是实现简单、幂等，重跑不会产生重复数据。如果要改成真正的增量，可以在 dbt 里用 `materialized='incremental'` + `unique_key`，让 dbt 生成 MERGE 语句，只更新或插入变化的行。

---

**Q4: 如何避免同一个职位被不同 API 抓到多次？**

A: 两层去重。第一层在 Python 里：`df.drop_duplicates(subset=["JOB_ID"])` 过滤同一个来源内的重复。第二层在 dbt 里：`stg_all_postings.sql` 用 `ROW_NUMBER() OVER (PARTITION BY company_name, job_title, date(listed_at))` 跨源去重，优先保留有薪资数据的那条。局限是如果同一个职位在不同源里 title 写法不一样（比如 "Data Engineer" vs "Senior Data Engineer I"），就去不掉。

---

### dbt & 数据转换

**Q5: dbt 的 staging 层和 mart 层有什么区别？你的项目里分别做了什么？**

A: Staging 层做清洗和标准化：字段重命名、类型转换、派生字段（用 title 推断 experience_level，用 description 推断 work_type），materialized 为 view。Mart 层做聚合：把 `stg_all_postings` 按不同维度（role、seniority、location、skill、company tier）聚合成分析结果，materialized 为 table，直接给 dashboard 查询。

---

**Q6: `stg_all_postings` 里的 ROW_NUMBER 去重逻辑解释一下。**

A: PARTITION BY `lower(company_name)`, `lower(job_title)`, `date(listed_at)` ——把公司+职位+日期完全相同的记录归为一组，认为是同一个职位。ORDER BY 里优先保留 `annual_salary_est IS NOT NULL` 的记录（`CASE WHEN...THEN 0 ELSE 1`），因为有薪资数据的记录更有价值；薪资相同时按 source 字母排序。`WHERE rn = 1` 只保留每组的第一条。

---

**Q7: 你怎么把各个数据源的薪资统一成年薪口径？**

A: 三个源的处理时机不同。JSearch 在 Python 抓取阶段（`fetch_jsearch.py` 的 `annualize()` 函数）就完成了年化，判断 `job_salary_period` 字段（HOUR/WEEK/MONTH/YEAR）后乘以对应系数（2080/52/12/1）。Adzuna 的 API 直接返回年薪，不需要年化，但 `SALARY_EST` 是在 Python 里算的 `(min + max) / 2`。Portal 数据没有薪资，全为 null。Staging 层把 `pay_period` 统一 hardcode 为 `'YEARLY'`，因为在 dbt 层已经不需要再转换了。

---

**Q8: 你的 skill 需求分析是怎么做的？**

A: 在 `mart_tech_skills.sql` 和 `mart_tech_skills_by_role.sql` 里，用一个 hardcoded 的技能关键词列表（约 50 个词）和 job description 做 cross join + ILIKE substring match，统计每个技能出现在多少个职位描述里。这是关键词匹配方法，不是 NLP/NER。优点是实现简单、Snowflake 原生支持；缺点是封闭集合（只能检测预定义的技能）、有 false positive 风险（比如短词可能匹配到不相关内容）、无法处理同义词（"PySpark" 和 "Spark with Python" 算两个不同技能）。

---

**Q9: 你的 `generate_schema_name` 宏做了什么，为什么需要它？**

A: dbt 默认行为是把 schema 拼接为 `{target.schema}_{custom_schema}` 的格式，比如如果 target.schema 是 `STAGING`，custom_schema 也是 `STAGING`，最后就会创建出 `STAGING_STAGING` 这个 schema。这个宏覆盖了默认行为，当 custom_schema_name 不为空时直接用 custom_schema_name（转大写），所以 staging 模型写进 `STAGING`，marts 写进 `MARTS`，不会出现重复的 schema 名。

---

**Q10: dbt `view` 和 `table` 的区别，你的项目里是怎么选择的？**

A: `view` 是查询时实时计算的逻辑定义，不存储数据，每次查询都要重新跑 SQL。`table` 是物理存储的预计算结果，查询速度快，但需要通过 `dbt run` 刷新。我的项目里 staging 层用 view，因为它们是中间计算步骤，被 mart 引用，不需要单独查询；marts 用 table，因为 dashboard 直接查询它们，需要低延迟响应，而且聚合计算（尤其是 cross join 的技能匹配）很重，不适合每次查询都重跑。

---

### 数据质量 & 工程规范

**Q11: 你是怎么保证数据质量的？**

A: 目前有几层保护。第一，fetch 脚本里有基础过滤（`WHERE JOB_ID IS NOT NULL AND TITLE IS NOT NULL`）。第二，staging 层用 `try_to_timestamp()` 处理时间字段，不会因为格式问题报错。第三，marts 里有薪资的合理范围过滤（`annual_salary_est BETWEEN 10000 AND 1000000`）。第四，`dbt test` 步骤会跑 schema.yml 里定义的 not_null 和 unique 测试。但**测试覆盖率不够**——三个实时 staging 模型和 `stg_all_postings` 都没有 dbt 测试，这是明显需要改进的地方。

---

**Q12: 如果今天没有数据写进来（API 全部失败），你有告警吗？**

A: 没有自动告警。GitHub Actions 会在 step 失败时把 workflow 标红，可以在 GitHub 界面看到，但没有配置邮件/Slack 通知。dbt 也没有配置 source freshness 检查（`dbt source freshness` 命令可以检测数据是否超过预期时间未更新）。这是个生产化前需要补的功能。

---

**Q13: 你的 pipeline 是幂等的吗（重跑两次结果一样吗）？**

A: fetch 脚本是幂等的，因为每次写入前先 `DELETE WHERE FETCHED_AT LIKE '{today}%'`，所以重跑只是把当天数据删了重写一遍，不会产生重复。dbt 的 `table` 模型也是幂等的，每次 `dbt run` 都会重建整张表（`CREATE OR REPLACE TABLE`）。`view` 模型本身就是 idempotent 的。所以整体 pipeline 重跑是安全的。

---

**Q14: 你的 state_code 跨源数据标准化是怎么做的？**

A: Adzuna 返回全州名（"California"），JSearch 返回两字母代码（"CA"）。标准化在 `mart_jobs_by_location.sql` 里做，用 50 个 CASE WHEN 枚举全部州名映射。**局限是这个映射只在这一张 mart 做了**，其他直接使用 state_code 字段的地方（比如想按州过滤数据）仍然有混合格式问题。更好的做法是在 staging 层统一标准化。

---

**Q15: 如果让你加一个新的数据源，你会怎么做？**

A: 四步走。第一，写一个新的 `fetch_{source}.py` 脚本，抓数据、处理薪资口径、写入 RAW schema 的一张新表（比如 `RAW.LINKEDIN_POSTINGS`）。第二，在 `staging/schema.yml` 的 sources 下注册这个新表，写一个 `stg_linkedin_postings.sql`，字段对齐到已有的 staging schema（job_id、company_name、job_title、annual_salary_est 等）。第三，在 `stg_all_postings.sql` 里加一个 CTE 引用新的 staging 模型，加入 UNION ALL。第四，更新 GitHub Actions workflow，加入新脚本的执行步骤。因为 `stg_all_postings` 是所有 mart 的统一入口，mart 层不需要改动。

---

**Q16: 你用 Snowflake 做了什么？为什么不用 BigQuery 或者 Redshift？**

A: 选 Snowflake 主要是因为它的 free trial 足够用来做这个规模的项目，而且 snowflake-connector-python 库很成熟，和 dbt 的集成也很好（`dbt-snowflake`）。技术上，Snowflake 的 `write_pandas()` 可以直接把 DataFrame 批量写入，`RLIKE` 支持 regex，`FLATTEN + PARSE_JSON` 可以在 SQL 里展开数组——这些特性在这个项目里都用到了。如果是公司环境，BigQuery 和 Redshift 都是完全合理的选择，架构不会有本质差别。

---

**Q17: 面试官说「你的项目没有增量加载，这在真实工作中是不可接受的，你怎么改？」**

A: 我完全同意这是个需要改进的地方。改法有几个层次：
- **dbt 层**：把 mart 模型改成 `materialized='incremental'`，指定 `unique_key='job_id'`，dbt 会生成 MERGE 语句只更新变化的行。
- **fetch 层**：在 fetch 脚本里加 watermark 逻辑，只拉 `updated_at > last_successful_run` 的数据，而不是每次都拉最近 30 天全量。需要持久化存储 last_run_time（比如写进 Snowflake 的一张 metadata 表）。
- **staging 层**：如果引入 CDC（Change Data Capture），可以用 Snowflake Streams 捕获 RAW 层的变化，只处理增量数据。
当前规模下全量日快照是 acceptable 的 tradeoff，但随着数据量增长，增量加载是必须要做的。

---

**Q18: 你的 `mart_remote_trend` 数据准不准？**

A: 坦白说，这张表有个 bug。staging 层存的 `remote_allowed` 值是 `'1'`、`'0'` 或 `'hybrid'`，但 mart 里 compare 的是 `'TRUE'` 和 `'FALSE'`，两边不匹配，导致 `remote_count` 和 `onsite_count` 永远是 0。修复很简单，把 `remote_allowed = 'TRUE'` 改成 `remote_allowed = '1'`，`remote_allowed = 'FALSE'` 改成 `remote_allowed = '0'`。这个 bug 暴露了测试覆盖不足的问题——如果在 staging schema.yml 里加了 `accepted_values` test，这种问题在 CI 里就能被抓到。

---

## 十一、一句话总结（面试开头/结尾用）

> "这是一个端到端的职位市场情报 pipeline：每天通过 GitHub Actions 从 Adzuna、JSearch、Greenhouse/Ashby 三个源抓取职位数据，写入 Snowflake RAW 层，再通过 dbt 做两层转换——staging 层标准化字段、跨源去重，marts 层按 role、seniority、location、company tier、技能需求做聚合，最终在 Streamlit dashboard 上展示分析结果。项目从设计到落地全程自己实现，解决的核心数据工程挑战是多源异构数据的字段标准化、薪资口径统一和跨源去重。"
