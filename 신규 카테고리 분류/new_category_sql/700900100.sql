SELECT id AS pid,
       CASE
           WHEN name LIKE '%스키%' THEN '700900100'
           WHEN lower(name) LIKE '%ski%' THEN '700900100'
           WHEN name LIKE '%보드%' THEN '700900400'
           WHEN lower(name) LIKE '%board%' THEN '700900400' 
           
           -- filter keywords: 스키

           WHEN keyword LIKE '%스키%' THEN '700900100'
           WHEN lower(keyword) LIKE '%ski%' THEN '700900100'
           WHEN name LIKE '%폴대%' THEN '700900100'
           WHEN keyword LIKE '%폴대%' THEN '700900100'
           WHEN name LIKE '%대회전%' THEN '700900100'
           WHEN keyword LIKE '%대회전%' THEN '700900100'
           WHEN name LIKE '%스틱%' THEN '700900100'
           WHEN keyword LIKE '%스틱%' THEN '700900100' 
           
           -- filter keywords: 스노우보드

           WHEN keyword LIKE '%보드%' THEN '700900400'
           WHEN lower(keyword) LIKE '%board%' THEN '700900400'
           WHEN name LIKE '%부츠%' THEN '700900400'
           WHEN keyword LIKE '%부츠%' THEN '700900400'
           WHEN name LIKE '%바인딩%' THEN '700900400'
           WHEN keyword LIKE '%바인딩%' THEN '700900400'
           WHEN name LIKE '%데크%' THEN '700900400'
           WHEN keyword LIKE '%데크%' THEN '700900400'
           WHEN name LIKE '%보더%' THEN '700900400'
           WHEN keyword LIKE '%보더%' THEN '700900400'
           WHEN name LIKE '%보그%' THEN '700900400'
           WHEN keyword LIKE '%보그%' THEN '700900400' 
           
           -- filter keywords: 애매

           WHEN name LIKE '%고글%' THEN '700900400'
           WHEN keyword LIKE '%고글%' THEN '700900400'
           WHEN name LIKE '%헬멧%' THEN '700900400'
           WHEN keyword LIKE '%헬멧%' THEN '700900400'
           WHEN name LIKE '%헬맷%' THEN '700900400'
           WHEN keyword LIKE '%헬맷%' THEN '700900400'
           WHEN name LIKE '%핼멧%' THEN '700900400'
           WHEN keyword LIKE '%핼멧%' THEN '700900400'
           WHEN name LIKE '%핼맷%' THEN '700900400'
           WHEN keyword LIKE '%핼맷%' THEN '700900400'
           ELSE '700900900'
       END AS new_category_id,
       name
FROM service1_quicket.product_info
WHERE category_id = 700900100
  AND status = 0