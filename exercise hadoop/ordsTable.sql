use classicmodels;

-- Create the ORDS table to hold 3M records --
drop table if exists ords;
create table ords as select * from orders where 1=2;
truncate table ords;
alter table ords change column comments UUID text;

-- Load 3M Records Using Stored Procedure --

DROP PROCEDURE IF EXISTS data_load;
DELIMITER $$
CREATE PROCEDURE data_load(p_rec_cnt INT)
BEGIN
	SET @loop_run:=1;
	SET @v_ordernumber:=10000;
	SET @v_base_date:='1960-01-01';

	START TRANSACTION;

	WHILE @loop_run <= p_rec_cnt DO

		SET @rand_days:=(SELECT FLOOR(RAND() * 5));
		SET @req_date:=ADDDATE(@v_base_date, @rand_days);
		SET @rand_days:=(SELECT FLOOR(RAND() * 5));
		SET @ship_date:=ADDDATE(@v_base_date, @rand_days);

		IF @rand_days > 2 THEN
			SET @status:='Shipped';
		ELSE
			SET @status:='Not Shipped';
		END IF;

		SET @comments:=(SELECT SUBSTR(CONCAT(MD5(RAND()),MD5(RAND())),1,36));
		SET @customernumber:=(SELECT FLOOR(RAND() * 123) * 321);

		INSERT INTO ords VALUES(@v_ordernumber, @v_base_date, @req_date, @ship_date, @status, @comments, @customernumber);

		SET @v_ordernumber:=@v_ordernumber+1;
		SET @v_base_date:=(SELECT ADDDATE(@v_base_date, 1));
		SET @loop_run:=@loop_run+1;

		IF @v_base_date >= '2016-12-31' THEN
			SET @v_base_date:='1960-01-01';
		END IF;

	END WHILE;
	COMMIT;
END$$
DELIMITER ;

CALL data_load(3000000);

select count(*) from ords;
exit;
