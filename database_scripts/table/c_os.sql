CREATE TABLE IF NOT EXISTS korzinka_db.c_os (
    id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    os_name VARCHAR(255),
    os_version VARCHAR(255),
    _lch TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()
);

--create primary key
DO $$ BEGIN
IF NOT EXISTS (
	SELECT 1
	FROM information_schema.table_constraints
	WHERE table_schema = 'korzinka_db'
	AND table_name = 'c_os'
	AND constraint_type = 'PRIMARY KEY'
	LIMIT 1
)
THEN
	ALTER TABLE IF EXISTS korzinka_db.c_os
		ADD CONSTRAINT pk_c_os_id
		PRIMARY KEY (id);
END IF;
END $$;

COMMENT ON TABLE korzinka_db.c_os IS 'reference table of operating systems';

COMMENT ON COLUMN korzinka_db.c_os.id IS 'identifier';
COMMENT ON COLUMN korzinka_db.c_os.os_name IS 'name';
COMMENT ON COLUMN korzinka_db.c_os.os_version IS 'version';
