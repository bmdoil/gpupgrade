package commanders

// Queries for data migration steps
const (
	PartitionIndexInitialize = `
	-- Generates a script to drop partition indexes that do not correspond to unique or primary
	-- constraints
	-- cte to hold the oid from all the root and child partition table
	WITH partitions (relid) AS
	(
		 SELECT DISTINCT
				parrelid
		 FROM
				pg_partition
		 UNION ALL
		 SELECT DISTINCT
				parchildrelid
		 FROM
				pg_partition_rule
	)
	,
	-- cte to hold the unique and primary key constraint on all the root and child partition table
	part_constraint AS
	(
		 SELECT
				conname,
				c.relname connrel,
				n.nspname relschema,
				cc.relname rel
		 FROM
				pg_constraint con
				JOIN
					 pg_depend dep
					 ON (refclassid, classid, objsubid) =
					 (
							'pg_constraint'::regclass,
							'pg_class'::regclass,
							0
					 )
					 AND refobjid = con.oid
					 AND deptype = 'i'
					 AND contype IN
					 (
							'u',
							'p'
					 )
				JOIN
					 pg_class c
					 ON objid = c.oid
					 AND relkind = 'i'
				JOIN
					 partitions
					 ON con.conrelid = partitions.relid
				JOIN
					 pg_class cc
					 ON cc.oid = partitions.relid
				JOIN
					 pg_namespace n
					 ON (n.oid = cc.relnamespace)
	)
	SELECT
	pg_catalog.quote_ident(n.nspname) as schema, pg_catalog.quote_ident(i.relname) as indexname, pg_catalog.quote_ident(y.relname) as tablename, $$ DROP INDEX IF EXISTS $$ || pg_catalog.quote_ident(n.nspname) ||'.'|| pg_catalog.quote_ident(i.relname) || $$ ;$$ as definition
	FROM
		 pg_index x
		 JOIN
				partitions c
				ON c.relid = x.indrelid
		 JOIN
				pg_class y
				ON c.relid = y.oid
		 JOIN
				pg_class i
				ON i.oid = x.indexrelid
		 LEFT JOIN
				pg_namespace n
				ON n.oid = y.relnamespace
		 LEFT JOIN
				pg_tablespace t
				ON t.oid = i.reltablespace
	WHERE
		 y.relkind = 'r'::char
		 AND i.relkind = 'i'::char
		 AND
		 (
				i.relname,
				n.nspname,
				y.relname
		 )
		 NOT IN
		 (
				SELECT
					 connrel,
					 relschema,
					 rel
				FROM
					 part_constraint
		 );
	`
	PartitionIndexFinalize = `
-- Generates SQL statements to create indexes on child partition tables that do
-- not correspond to primary or unique constraints.
WITH child_partitions_using_tsquery AS (
	SELECT DISTINCT pr.parchildrelid oid
	FROM pg_partition_rule pr
	JOIN pg_class pc ON pr.parchildrelid = pc.oid
	JOIN pg_attribute a ON a.attrelid = pc.oid
	WHERE a.atttypid = 'pg_catalog.tsquery'::pg_catalog.regtype
),
child_partitions (relid) AS (
	SELECT DISTINCT
	parchildrelid
	FROM
	pg_partition_rule
	WHERE
	parchildrelid NOT IN (SELECT oid FROM child_partitions_using_tsquery)
),
part_constraints AS (
	SELECT
	conname,
	c.relname conrel,
	n.nspname relschema,
	cc.relname rel
	FROM
	pg_constraint con
	JOIN
	pg_depend dep
	ON (dep.refclassid, dep.classid, dep.objsubid) = ('pg_constraint'::regclass, 'pg_class'::regclass, 0)
	AND dep.refobjid = con.oid
	AND dep.deptype = 'i'
	AND con.contype IN ('u','p')
	JOIN pg_class c ON dep.objid = c.oid AND c.relkind = 'i'
	JOIN child_partitions ON con.conrelid = child_partitions.relid
	JOIN pg_class cc ON cc.oid = con.conrelid
	JOIN pg_namespace n ON (n.oid = cc.relnamespace)
),
child_indexes AS
(
	SELECT
	n.nspname AS schemaname,
	c.relname AS tablename,
	i.relname AS indexname,
	t.spcname AS tablespace,
	pg_get_indexdef(i.oid) AS indexdef
	FROM
	pg_index x
	JOIN child_partitions np on np.relid = x.indrelid
	JOIN pg_class c ON c.oid = x.indrelid
	JOIN pg_class i ON i.oid = x.indexrelid
	LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
	LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace
	WHERE c.relkind = 'r'::char
	AND i.relkind = 'i'::char
	AND c.relhassubclass = 'f'
	AND x.indisunique = 'f'
),
-- Generates SQL statements to create indexes on root partition tables
-- that don't correspond to unique or primary key constraints
root_partitions_using_tsquery AS
(
	SELECT DISTINCT p.parrelid oid
	FROM pg_partition p
	JOIN pg_class pt ON p.parrelid = pt.oid
	JOIN pg_attribute a ON a.attrelid = pt.oid
	WHERE a.atttypid = 'pg_catalog.tsquery'::pg_catalog.regtype
),
root_partitions (relid) AS
(
	SELECT DISTINCT
	parrelid
	FROM
	pg_partition
	WHERE
	parrelid NOT IN (SELECT oid FROM root_partitions_using_tsquery)
),
root_constraints AS
(
	SELECT
	conname,
	c.relname conrel,
	n.nspname relschema,
	cc.relname rel
	FROM
	pg_constraint con
	JOIN pg_depend dep ON (dep.refclassid, dep.classid, dep.objsubid) = ('pg_constraint'::regclass,'pg_class'::regclass,0)
	AND dep.refobjid = con.oid
	AND dep.deptype = 'i'
	AND con.contype IN ('u','p')
	JOIN pg_class c ON dep.objid = c.oid AND c.relkind = 'i'
	JOIN root_partitions ON con.conrelid = root_partitions.relid
	JOIN pg_class cc ON cc.oid = con.conrelid
	JOIN pg_namespace n ON (n.oid = cc.relnamespace)
),
root_indexes AS
(
	SELECT
	n.nspname AS schemaname,
	c.relname AS tablename,
	i.relname AS indexname,
	t.spcname AS tablespace,
	pg_get_indexdef(i.oid) AS indexdef
	FROM
	pg_index x
	JOIN root_partitions rp on rp.relid = x.indrelid
	JOIN pg_class c ON c.oid = x.indrelid
	JOIN pg_class i ON i.oid = x.indexrelid
	LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
	LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace
	WHERE c.relkind = 'r'::char
	AND i.relkind = 'i'::char
)
SELECT
pg_catalog.quote_ident(schemaname) as schemaname,
pg_catalog.quote_ident(indexname) as indexname,
pg_catalog.quote_ident(tablename) as tablename,
indexdef as definition
FROM
child_indexes
WHERE (indexname, schemaname, tablename)
NOT IN (SELECT conrel, relschema, rel FROM part_constraints)
UNION ALL
SELECT
pg_catalog.quote_ident(schemaname) as schemaname,
pg_catalog.quote_ident(indexname) as indexname,
pg_catalog.quote_ident(tablename) as tablename,
$$SET SEARCH_PATH=$$ || schemaname || $$; $$ || indexdef || $$;$$ as definition
FROM
root_indexes
WHERE (indexname, schemaname, tablename)
NOT IN (SELECT conrel, relschema, rel FROM root_constraints);
`
	PartitionIndexRevert = `
	-- Generates SQL statements to create indexes on root partition tables
	-- that don't correspond to unique or primary key constraints
	-- cte to get all the unique and primary key constraints
	WITH root_partitions_using_tsquery AS
	(
		 SELECT DISTINCT p.parrelid oid
		 FROM pg_partition p
		 JOIN pg_class pt ON p.parrelid = pt.oid
		 JOIN pg_attribute a ON a.attrelid = pt.oid
		 WHERE a.atttypid = 'pg_catalog.tsquery'::pg_catalog.regtype
	)
	,
	root_partitions (relid) AS
	(
		 SELECT DISTINCT
				parrelid
		 FROM
				pg_partition
		 WHERE
				parrelid NOT IN (SELECT oid FROM root_partitions_using_tsquery)
	)
	,
	root_constraints AS
	(
		 SELECT
				conname,
				c.relname conrel,
				n.nspname relschema,
				cc.relname rel
		 FROM
				pg_constraint con
				JOIN
					 pg_depend dep
					 ON (dep.refclassid, dep.classid, dep.objsubid) =
					 (
							'pg_constraint'::regclass,
							'pg_class'::regclass,
							0
					 )
					 AND dep.refobjid = con.oid
					 AND dep.deptype = 'i'
					 AND con.contype IN
					 (
							'u',
							'p'
					 )
				JOIN
					 pg_class c
					 ON dep.objid = c.oid
					 AND c.relkind = 'i'
				JOIN
					 root_partitions
					 ON con.conrelid = root_partitions.relid
				JOIN
					 pg_class cc
					 ON cc.oid = con.conrelid
				JOIN
					 pg_namespace n
					 ON (n.oid = cc.relnamespace)
	)
	,
	root_indexes AS
	(
		 SELECT
				n.nspname AS schemaname,
				c.relname AS tablename,
				i.relname AS indexname,
				t.spcname AS tablespace,
				pg_get_indexdef(i.oid) AS indexdef
		 FROM
				pg_index x
				JOIN
					 root_partitions rp
					 on rp.relid = x.indrelid
				JOIN
					 pg_class c
					 ON c.oid = x.indrelid
				JOIN
					 pg_class i
					 ON i.oid = x.indexrelid
				LEFT JOIN
					 pg_namespace n
					 ON n.oid = c.relnamespace
				LEFT JOIN
					 pg_tablespace t
					 ON t.oid = i.reltablespace
		 WHERE
				c.relkind = 'r'::char
				AND i.relkind = 'i'::char
	),
	-- Generates SQL statements to create indexes on child partition tables that do
	-- not correspond to primary or unique constraints.
	child_partitions_using_tsquery AS
	(
		 SELECT DISTINCT pr.parchildrelid oid
		 FROM pg_partition_rule pr
		 JOIN pg_class pc ON pr.parchildrelid = pc.oid
		 JOIN pg_attribute a ON a.attrelid = pc.oid
		 WHERE a.atttypid = 'pg_catalog.tsquery'::pg_catalog.regtype
	)
	,
	child_partitions (relid) AS
	(
		 SELECT DISTINCT
				parchildrelid
		 FROM
				pg_partition_rule
		 WHERE
				parchildrelid NOT IN (SELECT oid FROM child_partitions_using_tsquery)
	)
	,
	part_constraints AS
	(
		 SELECT
				conname,
				c.relname conrel,
				n.nspname relschema,
				cc.relname rel
		 FROM
				pg_constraint con
				JOIN
					 pg_depend dep
					 ON (dep.refclassid, dep.classid, dep.objsubid) =
					 (
							'pg_constraint'::regclass,
							'pg_class'::regclass,
							0
					 )
					 AND dep.refobjid = con.oid
					 AND dep.deptype = 'i'
					 AND con.contype IN
					 (
							'u',
							'p'
					 )
				JOIN
					 pg_class c
					 ON dep.objid = c.oid
					 AND c.relkind = 'i'
				JOIN
					 child_partitions
					 ON con.conrelid = child_partitions.relid
				JOIN
					 pg_class cc
					 ON cc.oid = con.conrelid
				JOIN
					 pg_namespace n
					 ON (n.oid = cc.relnamespace)
	)
	,
	child_indexes AS
	(
		 SELECT
				n.nspname AS schemaname,
				c.relname AS tablename,
				i.relname AS indexname,
				t.spcname AS tablespace,
				pg_get_indexdef(i.oid) AS indexdef
		 FROM
				pg_index x
				JOIN
					 child_partitions np
					 on np.relid = x.indrelid
				JOIN
					 pg_class c
					 ON c.oid = x.indrelid
				JOIN
					 pg_class i
					 ON i.oid = x.indexrelid
				LEFT JOIN
					 pg_namespace n
					 ON n.oid = c.relnamespace
				LEFT JOIN
					 pg_tablespace t
					 ON t.oid = i.reltablespace
		 WHERE
				c.relkind = 'r'::char
				AND i.relkind = 'i'::char
	)
	SELECT
	pg_catalog.quote_ident(schemaname) as schemaname, pg_catalog.quote_ident(indexname) as indexname, pg_catalog.quote_ident(tablename) as tablename, $$SET SEARCH_PATH=$$ || schemaname || $$; $$ || indexdef || $$;$$ as definition
	FROM
		 root_indexes
	WHERE
		 (
				indexname,
				schemaname,
				tablename
		 )
		 NOT IN
		 (
				SELECT
					 conrel,
					 relschema,
					 rel
				FROM
					 root_constraints
		 )
	UNION ALL
	SELECT
	pg_catalog.quote_ident(schemaname) as schemaname, pg_catalog.quote_ident(indexname) as indexname, pg_catalog.quote_ident(tablename) as tablename, 'DO $$ BEGIN IF NOT EXISTS ( SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE  c.relname = ''' || indexname ||
	''' AND n.nspname = ''' || schemaname || ''' ) THEN SET SEARCH_PATH=' || schemaname || '; ' || indexdef || '; END IF; END $$; ' as definition
	FROM
		 child_indexes
	WHERE
		 (
				indexname,
				schemaname,
				tablename
		 )
		 NOT IN
		 (
				SELECT
					 conrel,
					 relschema,
					 rel
				FROM
					 part_constraints
		 );
	`
)