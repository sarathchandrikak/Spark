-- 1: Find the average age of people who died and who survived
spark.sql("SELECT avg(_c4) FROM data where (_c2 = 1) and (_c4 != 'NA')").show
spark.sql("SELECT avg(_c4) FROM data where (_c2 = 0) and (_c4 != 'NA')").show

-- 2:	Number of males and females survived in following age range: (age <= 20), (20 < age <= 50) and (age > 50 and age = NA)
spark.sql("SELECT count(*) FROM data where (_c10 = 'male') and (_c4 != 'NA') and (_c4 <= 20)").show
spark.sql("SELECT count(*) FROM data where (_c10 = 'female') and (_c4 != 'NA') and (_c4 <= 20)").show
spark.sql("SELECT count(*) FROM data where (_c10 = 'male') and (_c4 != 'NA') and (_c4 > 20) and (_c4 <= 50)").show
spark.sql("SELECT count(*) FROM data where (_c10 = 'female') and (_c4 != 'NA') and (_c4 > 20) and (_c4 <= 50)").show

-- 3:	embarked locations and their count
spark.sql("SELECT _c5, count(*) as cnt FROM data group by _c5 order by cnt").show

-- 4:	Number of people survived in each class
spark.sql("SELECT _c1, count(*) FROM data where (_c2 = 1) group by _c1").show

-- 5:	Number of males survived whose age is less than 30 and travelling in 2nd class
spark.sql("SELECT count(*) FROM data where (_c2 = 1) and (_c1 = '2nd') and (_c4 != 'NA') and (_c4 < 30)").show
