-- 1: No of athletes participated in each Olympic event
spark.sql("SELECT _c3, count(*) FROM olympic group by _c3").show

-- 2: No of medals each country won in each Olympic in ascending order
spark.sql("SELECT _c2, _c3, sum(_c9) as cnt FROM olympic group by _c2, _c3 order by _c3, cnt DESC").show
spark.sql("SELECT _c2, _c3, sum(_c9) as cnt FROM olympic group by _c2, _c3 order by cnt").show

-- 3: Top 10 athletes who won highest gold medals in all the Olympic events
spark.sql("SELECT _c0, sum(_c9) as cnt FROM olympic group by _c0 order by cnt DESC").show

-- 4: No of athletes who won gold and whose age is less than 20
spark.sql("SELECT count(*) FROM olympic where _c1 < 20 and _c6 > 0").show

-- 5: Youngest athlete who won gold in each category of sports in each Olympic
spark.sql("SELECT * FROM olympic WHERE _c1 = (SELECT MIN(_c1) FROM olympic WHERE _c1 > 0) and _c6 > 0").show

-- 6: No of atheletes from each country who has won a medal in each Olympic in each sports
spark.sql("SELECT _c2, _c3, _c5, count(*) as cnt FROM olympic group by _c2, _c3, _c5 order by _c3, cnt DESC").show

-- 7: No of athletes won at least a medal in each events in all the Olympics
spark.sql("SELECT DISTINCT _c5, count(*) as cnt FROM olympic group by _c5 order by cnt DESC").show

-- 8: Country won highest no of medals in wrestling in 2012
spark.sql("SELECT  _c2, sum(_c9) as cnt FROM olympic  where _c3 = '2012' and _c5 = 'Wrestling' group by _c2 order by cnt DESC").show
