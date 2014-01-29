\i setup_db.sql;
vacuum;
analyze;

--Problem 1a--
SELECT COUNT(ic)
FROM pg_class rc, pg_class ic, pg_index i
WHERE rc.relname = 'customers'
      AND rc.oid = i.indrelid
      AND ic.oid = i.indexrelid;




--Problem 1b--	  
SELECT c.relname, c.relpages
FROM pg_class c
WHERE c.relname IN (SELECT relname FROM pg_stat_user_tables)
      AND c.relkind = 'r'
ORDER BY c.relpages DESC;

SELECT ic.relname, ic.relpages
FROM pg_class rc, pg_class ic, pg_index i
WHERE rc.relname IN (SELECT relname FROM pg_stat_user_tables)
      AND rc.oid = i.indrelid
      AND ic.oid = i.indexrelid
ORDER BY ic.relpages DESC;


--Problem 1c--
SELECT attname, -1*n_distinct*(SELECT reltuples
			       FROM pg_class 
			       WHERE relname = 'customers')
FROM pg_stats
WHERE tablename = 'customers'
      AND n_distinct < 0
UNION
SELECT attname, n_distinct
FROM pg_stats
WHERE tablename = 'customers'
      AND n_distinct > 0;

--Problem 1d--
select
 count (distinct address1) as address1,
 count (distinct address2) as address2,
 count (distinct age) as age,
 count (distinct city) as city,
 count (distinct country) as country,
 count (distinct creditcard) as creditcard,
 count (distinct creditcardexpiration) as creditcardexpiration,
 count (distinct creditcardtype) as creditcardtype,
 count (distinct customerid) as customerid,
 count (distinct email) as email,
 count (distinct firstname) as firstname,
 count (distinct gender) as gender,
 count (distinct income) as income,
 count (distinct lastname) as lastname,
 count (distinct password) as password,
 count (distinct phone) as phone,
 count (distinct region) as region,
 count (distinct state) as state,
 count (distinct username) as username,
 count (distinct zip) as zip
from customers;



--Problem 2a--
\i setup_db.sql;
vacuum;
analyze;
EXPLAIN SELECT * FROM customers WHERE country = 'Japan';


--Problem 3--
CREATE INDEX customers_country ON customers(country);
vacuum;
analyze;

--Problem 3a--
SELECT relname, relpages
FROM pg_class
WHERE relname = 'customers_country';

--Problem 3b--
EXPLAIN SELECT * FROM customers WHERE country = 'Japan';

--Problem 3c--
CLUSTER customers_country ON customers;
vacuum;
analyze;
EXPLAIN SELECT * FROM customers WHERE country = 'Japan';



\i setup_db.sql;
vacuum;
analyze;

--Problem 4a--
explain select totalamount from customers c, orders o where 
c.customerid = o.customerid and c.country = 'Japan';

--Problem 4c--
set enable_hashjoin to 'off';
explain select totalamount from customers c, orders o where 
c.customerid = o.customerid and c.country = 'Japan';

--Problem 4d--
set enable_mergejoin to 'off';
explain select totalamount from customers c, orders o where 
c.customerid = o.customerid and c.country = 'Japan';


\i setup_db.sql;
vacuum;
analyze;

--Problem 5a--
explain select avg(totalamount) as avgOrder, country
from customers c, orders o where c.customerid = o.customerid
group by country order by avgOrder;

set enable_hashjoin to 'off';
explain select avg(totalamount) as avgOrder, country
from customers c, orders o where c.customerid = o.customerid
group by country order by avgOrder;

--Problem 5b--
set enable_hashjoin to 'on';
explain select * from customers c, orders o
where c.customerid = o.customerid order by c.customerid;

set enable_mergejoin to 'off';
explain select * from customers c, orders o
where c.customerid = o.customerid order by c.customerid;



--Problem 6a--
\i setup_db.sql;
vacuum;
analyze;
EXPLAIN SELECT C.customerid, C.lastname
	FROM customers C
	WHERE 4 < (SELECT COUNT(*)
	      	   FROM orders O
		   WHERE O.customerid = C.customerid);

--Problem 6b--
CREATE VIEW ordercount AS
       SELECT o.customerid, COUNT(*) AS numorders
       FROM customers c, orders o
       WHERE c.customerid = o.customerid
       GROUP BY o.customerid;

--Problem 6c--
EXPLAIN SELECT c.customerid, c.lastname
	FROM customers c, ordercount o
	WHERE c.customerid = o.customerid
	      AND 4 < o.numorders;



--Problem 7a--
\i setup_db.sql;
vacuum;
analyze;
EXPLAIN SELECT customerid, lastname, numorders
	FROM (SELECT C.customerid, C.lastname, COUNT(*) AS numorders
	      FROM Customers C, Orders O
	      WHERE C.customerid = O.customerid
	      	    AND C.country = 'Japan'
	      GROUP BY C.customerid, lastname) AS ORDERCOUNTS1
	WHERE 5 >= (SELECT COUNT(*)
	      	    FROM (SELECT C.customerid, C.lastname, COUNT(*) AS numorders
		    	  FROM Customers C, Orders O
			  WHERE C.customerid = O.customerid
			  	AND C.country = 'Japan'
			  GROUP BY C.customerid, lastname) AS ORDERCOUNTS2
		    WHERE ORDERCOUNTS1.numorders < ORDERCOUNTS2.numorders)
        ORDER BY customerid;


--Problem 7b--
CREATE VIEW OrderCountJapan AS
       SELECT o.customerid, COUNT(*) AS numorders
       FROM customers c, orders o
       WHERE c.customerid = o.customerid
       	     AND c.country = 'Japan'
       GROUP BY o.customerid;

CREATE VIEW MoreFrequentJapanCustomers AS
       SELECT ocj1.customerid, COUNT(*) AS oRank
       FROM OrderCountJapan ocj1, OrderCountJapan ocj2
       WHERE ocj1.numorders < ocj2.numorders
       GROUP BY ocj1.customerid;

--Problem 7c--
EXPLAIN SELECT C.customerid, C.lastname, OCJ.numorders
	FROM customers C, MoreFrequentJapanCustomers MFJC, OrderCountJapan OCJ
	WHERE C.customerid = MFJC.customerid
	      AND C.customerid = OCJ.customerid
	      AND MFJC.oRank <= 5;

