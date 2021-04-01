-- 최근 n일간의 검색 로그 데이터
-- user_id당 이전에 검색한 키워드 prev_keyword와 그 후에 검색한 키워드 next_keyword 가져오기
-- prev_keyword, 현재 keyword, next_keyword의 검색 시간을 각각 prev_server_time, new_server_time, next_server_time으로 지정
-- cur_prev_duration = cur_server_time - prev_server_time
-- prev_next_duration = cur_server_time - prev_server_time
DROP TABLE IF EXISTS workspace.search_term_log_modified;


CREATE TABLE workspace.search_term_log_modified AS
SELECT user_id,
       session_id,
       lower(trim(regexp_replace(regexp_replace(search_term, '\\r|\\n', ' '), '\\s+', ' '))) AS cur_keyword,
       LAG(cur_keyword, 1) OVER (PARTITION BY user_id ORDER BY server_time_kst) AS prev_keyword,
       LEAD(cur_keyword, 1) OVER (PARTITION BY user_id ORDER BY server_time_kst) AS next_keyword,
       REPLACE(cur_keyword, ' ', '') AS cur_keyword_no_space,
       REPLACE(prev_keyword, ' ', '') AS prev_keyword_no_space,
       REPLACE(next_keyword, ' ', '') AS next_keyword_no_space,
       server_time_kst::TIMESTAMP AS cur_server_time,
       LAG(server_time_kst, 1) OVER (PARTITION BY user_id ORDER BY server_time_kst)::TIMESTAMP AS prev_server_time,
       LEAD(server_time_kst, 1) OVER (PARTITION BY user_id ORDER BY server_time_kst)::TIMESTAMP AS next_server_time,
       datediff(SECOND, prev_server_time, cur_server_time) AS cur_prev_duration,
       datediff(SECOND, prev_server_time, next_server_time) AS prev_next_duration
FROM bun_log_db.app_event_type_search
WHERE YEAR || MONTH || DAY >= {initial_date}
  AND YEAR || MONTH || DAY < {final_date}
  AND search_term IS NOT NULL
  AND search_term != ''
  AND search_term != ' '
  AND user_id > 0
  AND device_type IN ('a',
                      'i');

-- 최근 n일간의 키워드 검색 횟수
DROP TABLE IF EXISTS workspace.keyword_search_count;


CREATE TABLE workspace.keyword_search_count AS
SELECT cur_keyword AS keyword,
       count(*) AS search_count
FROM workspace.search_term_log_modified
GROUP BY 1;


-- prev_keyword -> cur_keyword 검색 횟수
-- cur_keyword 를 prev_keyword의 연관검색어(related_keyword)로 지정
DROP TABLE IF EXISTS workspace.related_keyword_prev_cur;


CREATE TABLE workspace.related_keyword_prev_cur AS
SELECT prev_keyword AS keyword,
       cur_keyword AS related_keyword,
       COUNT(*) AS COUNT
FROM workspace.search_term_log_modified
WHERE prev_keyword IS NOT NULL
  AND cur_prev_duration <= 600
  AND cur_prev_duration >= 1
  AND cur_keyword_no_space != prev_keyword_no_space
GROUP BY 1,
         2;


-- prev_keyword -> next_keyword 검색 횟수
-- next_keyword 를 prev_keyword의 연관검색어(related_keyword)로 지정
DROP TABLE IF EXISTS workspace.related_keyword_prev_next;


CREATE TABLE workspace.related_keyword_prev_next AS
SELECT prev_keyword AS keyword,
       next_keyword AS related_keyword,
       COUNT(*) AS COUNT
FROM workspace.search_term_log_modified
WHERE prev_keyword IS NOT NULL
  AND prev_next_duration <= 900
  AND prev_next_duration >= 1
  AND next_keyword_no_space != prev_keyword_no_space
GROUP BY 1,
         2;


-- prev_keyword -> cur_keyword의 검색 횟와 prev_keyword -> next_keyword의 검색 횟수 합치기
DROP TABLE IF EXISTS workspace.related_keyword_combined;


CREATE TABLE workspace.related_keyword_combined AS
SELECT c.keyword,
       c.related_keyword,
       c.count + n.count AS related_count,
       sc.search_count,
       rsc.search_count AS related_search_count,

  (SELECT COUNT(*)
   FROM workspace.search_term_log_modified) AS total_search_count,
       related_count / total_search_count :: float AS support,
       related_count / sc.search_count :: float AS confidence,
       confidence / (rsc.search_count / total_search_count :: float) AS lift
FROM workspace.related_keyword_prev_cur c
JOIN workspace.related_keyword_prev_next n ON c.keyword = n.keyword
AND c.related_keyword = n.related_keyword
JOIN workspace.keyword_search_count sc ON c.keyword = sc.keyword
JOIN workspace.keyword_search_count rsc ON c.related_keyword = rsc.keyword
WHERE related_count > 2;


-- minimum confidence 와 min lift를 지정하여 필터하기
-- ex)
-- minimum confidence = 1/(keyword_search_count)^0.6
-- minimum lift = 1
DROP TABLE IF EXISTS workspace.related_keyword_df_filtered;


CREATE TABLE workspace.related_keyword_df_filtered AS
SELECT *
FROM workspace.related_keyword_combined
WHERE confidence > 1 / pow(search_count, {n})
  AND lift > {min_lift};


-- 키워드별 confidence와 lift의 max
DROP TABLE IF EXISTS workspace.related_keyword_df_max;


CREATE TABLE workspace.related_keyword_df_max AS
SELECT keyword,
       max(confidence) AS max_confidence,
       max(lift) AS max_lift
FROM workspace.related_keyword_df_filtered
GROUP BY 1;


-- 연관검색에 사용될 데이터
-- score = (confidence_weight * confidence / max_confidence) + (lift_weight * lift / max_lift)
DROP TABLE IF EXISTS workspace.related_keyword_df;


CREATE TABLE workspace.related_keyword_df AS
SELECT a.keyword,
       a.related_keyword,
       a.related_count,
       a.search_count,
       ROUND(({confidence_weight} * a.confidence / b.max_confidence:: float) + ({lift_weight} * a.lift / b.max_lift:: float), 4) AS score
FROM workspace.related_keyword_df_filtered a
JOIN workspace.related_keyword_df_max b ON a.keyword = b.keyword
ORDER BY a.search_count DESC,
         a.keyword,
         score DESC;