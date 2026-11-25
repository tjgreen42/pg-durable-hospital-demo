\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA IF NOT EXISTS hospital_refresh_lab;
SET LOCAL search_path TO hospital_refresh_lab, public;

CREATE TABLE IF NOT EXISTS agents (
    agent text PRIMARY KEY, active_id bigint
);
CREATE TABLE IF NOT EXISTS required_pages (
    agent text REFERENCES agents, url text, PRIMARY KEY (agent,url)
);
CREATE TABLE IF NOT EXISTS releases (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    agent text NOT NULL REFERENCES agents, parent_id bigint REFERENCES releases,
    mode text NOT NULL CHECK (mode IN ('chunks','models')),
    embedding_model text NOT NULL, chat_model text NOT NULL,
    state text NOT NULL DEFAULT 'staged' CHECK (state IN ('staged','building','ready','rejected')),
    decision text, coordinator text, deadline timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS jobs (
    release_id bigint REFERENCES releases, kind text, pipeline text NOT NULL UNIQUE,
    instance text, PRIMARY KEY (release_id,kind)
);
ALTER TABLE agents ADD COLUMN IF NOT EXISTS generation bigint NOT NULL DEFAULT 0;
ALTER TABLE releases ADD COLUMN IF NOT EXISTS parent_generation bigint NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS passages (
    release_id bigint REFERENCES releases, unit_id bigint, url text NOT NULL,
    ordinal int NOT NULL, content text NOT NULL, embedding public.vector(1536),
    PRIMARY KEY (release_id,unit_id), UNIQUE(release_id,url,ordinal)
);
CREATE TABLE IF NOT EXISTS benefits (
    release_id bigint REFERENCES releases, url text, fields jsonb NOT NULL,
    PRIMARY KEY (release_id,url)
);
CREATE OR REPLACE VIEW live_passages AS
  SELECT a.agent,p.* FROM agents a JOIN passages p ON p.release_id=a.active_id;
CREATE OR REPLACE VIEW live_benefits AS
  SELECT a.agent,b.* FROM agents a JOIN benefits b ON b.release_id=a.active_id;

-- One immutable candidate per call. Its inputs, pipeline watermarks and outputs cannot
-- overlap another candidate. Per-release objects are a deliberate lab simplification.
CREATE OR REPLACE FUNCTION stage(p_agent text, pages jsonb, p_mode text,
  embed_alias text DEFAULT 'default-embedding', chat_alias text DEFAULT 'default-chat')
RETURNS bigint LANGUAGE plpgsql SET search_path=hospital_refresh_lab,public AS $$
DECLARE r bigint; prefix text; parent bigint; gen bigint; k text; src text; steps jsonb[]; sink jsonb;
BEGIN
  SELECT active_id,generation INTO STRICT parent,gen FROM agents WHERE agent=p_agent;
  IF jsonb_typeof(pages) IS DISTINCT FROM 'array' OR jsonb_array_length(pages)=0
    THEN RAISE EXCEPTION 'Supply a nonempty JSON array of pages'; END IF;
  INSERT INTO releases(agent,parent_id,parent_generation,mode,embedding_model,chat_model)
    VALUES(p_agent,parent,gen,p_mode,embed_alias,chat_alias) RETURNING id INTO r;
  prefix := 'hospital_r'||r||'_';
  EXECUTE format('CREATE TABLE hospital_refresh_lab.%I
    (id bigint PRIMARY KEY, url text NOT NULL UNIQUE, content text NOT NULL CHECK(length(btrim(content))>0),
     is_plan boolean NOT NULL)',prefix||'pages');
  EXECUTE format('INSERT INTO hospital_refresh_lab.%I SELECT * FROM jsonb_to_recordset($1)
    AS p(id bigint,url text,content text,is_plan boolean)',prefix||'pages') USING pages;

  -- Compute chunks in page context. Pipeline-generated doc_id is batch-local.
  EXECUTE format('CREATE TABLE hospital_refresh_lab.%I AS
    SELECT row_number() OVER(ORDER BY p.id,c.chunk_index)::bigint AS id,
           p.url,c.chunk_index,c.chunk_text
    FROM hospital_refresh_lab.%I p CROSS JOIN LATERAL
         azure_ai.chunk(p.content, ''recursive'', 32, 4) c',prefix||'units',prefix||'pages');
  IF p_mode='models' THEN
    EXECUTE format('CREATE TABLE hospital_refresh_lab.%I AS SELECT * FROM hospital_refresh_lab.%I WHERE is_plan',
                   prefix||'plans',prefix||'pages');
    EXECUTE format('CREATE TABLE hospital_refresh_lab.%I (id bigint PRIMARY KEY,embedding public.vector(1536))',prefix||'vectors');
    EXECUTE format('CREATE TABLE hospital_refresh_lab.%I (id bigint PRIMARY KEY,metadata jsonb)',prefix||'fields');
  END IF;
  FOREACH k IN ARRAY CASE WHEN p_mode='models' THEN ARRAY['chunks','vectors','fields'] ELSE ARRAY['chunks'] END LOOP
    CASE k
      WHEN 'chunks' THEN
        src:='pages';
        steps:=ARRAY[ai.chunk_step(input=>'content',chunk_size=>32,overlap=>4)];
        sink:=ai.table_sink(table_name=>prefix||k,schema_name=>'hospital_refresh_lab');
      WHEN 'vectors' THEN
        src:='units';
        steps:=ARRAY[ai.embed_step(input=>'chunk_text',model=>embed_alias,dimensions=>1536)];
        sink:=ai.table_sink(table_name=>prefix||k,schema_name=>'hospital_refresh_lab',
           on_conflict=>ARRAY['id'],on_conflict_action=>'DO UPDATE SET embedding=EXCLUDED.embedding');
      WHEN 'fields' THEN
        src:='plans';
        steps:=ARRAY[ai.extract_step(input=>'content',model=>chat_alias,
          data=>ARRAY['plan_name: string - plan name as written','deductible_usd: number - annual deductible in US dollars'])];
        sink:=ai.table_sink(table_name=>prefix||k,schema_name=>'hospital_refresh_lab',
           on_conflict=>ARRAY['id'],on_conflict_action=>'DO UPDATE SET metadata=EXCLUDED.metadata');
    END CASE;
    INSERT INTO jobs(release_id,kind,pipeline) VALUES(r,k,prefix||k);
    PERFORM ai.create_pipeline(name=>prefix||k,
      source=>ai.table_source(table_name=>prefix||src,schema_name=>'hospital_refresh_lab',incremental_column=>'id'),
      steps=>steps,sink=>sink,trigger=>'manual');
  END LOOP;
  RETURN r;
END $$;

CREATE OR REPLACE FUNCTION waiting(r bigint) RETURNS boolean LANGUAGE sql VOLATILE AS $$
  SELECT clock_timestamp()<s.deadline AND EXISTS
    (SELECT 1 FROM hospital_refresh_lab.jobs j WHERE j.release_id=r AND
       coalesce(df.status(j.instance),'pending') NOT IN ('completed','failed','cancelled','canceled'))
  FROM hospital_refresh_lab.releases s WHERE s.id=r
$$;

CREATE OR REPLACE FUNCTION finish(r bigint) RETURNS text LANGUAGE plpgsql SET search_path=hospital_refresh_lab,public AS $$
DECLARE s releases%ROWTYPE; prefix text:='hospital_r'||r||'_'; current_id bigint; gen bigint; reason text; mismatch boolean;
BEGIN
  SELECT * INTO STRICT s FROM releases WHERE id=r FOR UPDATE;
  IF s.state IN ('ready','rejected') THEN RETURN s.state; END IF;
  IF s.state<>'building' THEN RAISE EXCEPTION 'Candidate % not submitted',r; END IF;
  SELECT active_id,generation INTO STRICT current_id,gen FROM agents WHERE agent=s.agent FOR UPDATE;
  SELECT string_agg(j.kind||': '||coalesce(df.status(j.instance),'unknown'),', ') INTO reason
    FROM jobs j WHERE j.release_id=r AND df.status(j.instance) IS DISTINCT FROM 'completed';
  IF reason IS NOT NULL THEN reason:='Children not completed: '||reason;
  ELSIF current_id IS DISTINCT FROM s.parent_id OR gen<>s.parent_generation THEN reason:='Active version changed; refusing stale promotion';
  ELSE
    EXECUTE format('SELECT EXISTS (SELECT url FROM hospital_refresh_lab.required_pages WHERE agent=$1
                   EXCEPT SELECT url FROM hospital_refresh_lab.%I)',prefix||'pages') INTO mismatch USING s.agent;
    IF mismatch THEN reason:='Required page missing from scrape'; END IF;
  END IF;
  IF reason IS NULL THEN
    -- Multiset equality preserves repeated text; a successful pipeline status alone is not sufficient.
    EXECUTE format('SELECT NOT EXISTS(SELECT 1 FROM hospital_refresh_lab.%1$I) OR EXISTS(
      (SELECT chunk_text FROM hospital_refresh_lab.%1$I EXCEPT ALL SELECT chunk_text FROM hospital_refresh_lab.%2$I)
      UNION ALL
      (SELECT chunk_text FROM hospital_refresh_lab.%2$I EXCEPT ALL SELECT chunk_text FROM hospital_refresh_lab.%1$I))
      OR EXISTS(SELECT url FROM hospital_refresh_lab.%3$I EXCEPT SELECT url FROM hospital_refresh_lab.%1$I)',
      prefix||'units',prefix||'chunks',prefix||'pages') INTO mismatch;
    IF mismatch THEN reason:='Chunk output differs from page-keyed source manifest'; END IF;
  END IF;
  IF reason IS NULL AND s.mode='models' THEN
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM hospital_refresh_lab.%1$I u FULL JOIN hospital_refresh_lab.%2$I v USING(id)
           WHERE u.id IS NULL OR v.embedding IS NULL OR vector_dims(v.embedding)<>1536)
      OR NOT EXISTS(SELECT 1 FROM hospital_refresh_lab.%3$I)
      OR EXISTS(SELECT 1 FROM hospital_refresh_lab.%3$I p FULL JOIN hospital_refresh_lab.%4$I f USING(id)
           WHERE p.id IS NULL OR f.id IS NULL OR jsonb_typeof(f.metadata->''plan_name'') IS DISTINCT FROM ''string''
           OR btrim(f.metadata->>''plan_name'')='''' OR jsonb_typeof(f.metadata->''deductible_usd'') IS DISTINCT FROM ''number'')',
      prefix||'units',prefix||'vectors',prefix||'plans',prefix||'fields') INTO mismatch;
    IF mismatch THEN reason:='Missing/misaligned embedding or required benefit fields'; END IF;
  END IF;
  IF reason IS NOT NULL THEN
    UPDATE releases SET state='rejected',decision=reason WHERE id=r;
    RETURN 'rejected: '||reason;
  END IF;
  IF s.mode='models' THEN
    EXECUTE format('INSERT INTO hospital_refresh_lab.passages SELECT $1,u.id,u.url,u.chunk_index,u.chunk_text,v.embedding
      FROM hospital_refresh_lab.%I u JOIN hospital_refresh_lab.%I v USING(id)',prefix||'units',prefix||'vectors') USING r;
    EXECUTE format('INSERT INTO hospital_refresh_lab.benefits SELECT $1,p.url,f.metadata
      FROM hospital_refresh_lab.%I p JOIN hospital_refresh_lab.%I f USING(id)',prefix||'plans',prefix||'fields') USING r;
  ELSE
    EXECUTE format('INSERT INTO hospital_refresh_lab.passages(release_id,unit_id,url,ordinal,content)
      SELECT $1,id,url,chunk_index,chunk_text FROM hospital_refresh_lab.%I',prefix||'units') USING r;
  END IF;
  UPDATE agents SET active_id=r,generation=generation+1 WHERE agent=s.agent;
  UPDATE releases SET state='ready',decision='Published atomically' WHERE id=r;
  RETURN 'published';
END $$;

CREATE OR REPLACE FUNCTION submit(r bigint) RETURNS text LANGUAGE plpgsql SET search_path=hospital_refresh_lab,public AS $$
DECLARE s releases%ROWTYPE; j jobs%ROWTYPE; child text; coordinator_id text;
BEGIN
  SELECT * INTO STRICT s FROM releases WHERE id=r FOR UPDATE;
  IF s.coordinator IS NOT NULL THEN RETURN s.coordinator; END IF;
  IF s.state<>'staged' THEN RAISE EXCEPTION 'Not a staged candidate: %',r; END IF;
  FOR j IN SELECT * FROM jobs WHERE release_id=r ORDER BY kind LOOP
    PERFORM ai.run(j.pipeline);
    SELECT last_run_instance_id INTO STRICT child FROM ai.status(j.pipeline);
    IF child IS NULL THEN RAISE EXCEPTION 'No durable instance for pipeline %',j.pipeline; END IF;
    UPDATE jobs SET instance=child WHERE release_id=r AND kind=j.kind;
  END LOOP;
  coordinator_id:=df.start(df.seq(
      df.loop(df.sleep(2),format('SELECT hospital_refresh_lab.waiting(%s)',r),false),
      df.sql(format('SELECT hospital_refresh_lab.finish(%s)',r))), 'hospital_demo-publish-'||r);
  UPDATE releases SET state='building',coordinator=coordinator_id,deadline=clock_timestamp()+interval '5 minutes' WHERE id=r;
  RETURN coordinator_id;
END $$;

CREATE OR REPLACE FUNCTION switch_version(p_agent text, expected bigint, target bigint)
RETURNS void LANGUAGE plpgsql SET search_path=hospital_refresh_lab,public AS $$
BEGIN
  IF NOT EXISTS(SELECT 1 FROM releases WHERE id=target AND agent=p_agent AND state='ready') THEN
    RAISE EXCEPTION 'Target is not a previously published snapshot for this agent'; END IF;
  UPDATE agents SET active_id=target,generation=generation+1 WHERE agent=p_agent AND active_id=expected;
  IF NOT FOUND THEN RAISE EXCEPTION 'Active version changed'; END IF;
END $$;

CREATE OR REPLACE FUNCTION search_chunks(p_agent text,q text)
RETURNS TABLE(release_id bigint,url text,ordinal int,content text) LANGUAGE sql STABLE AS $$
  SELECT p.release_id,p.url,p.ordinal,p.content FROM hospital_refresh_lab.live_passages p
  WHERE p.agent=p_agent AND to_tsvector('english',p.content) @@ plainto_tsquery('english',q)
  ORDER BY ts_rank(to_tsvector('english',p.content),plainto_tsquery('english',q)) DESC,p.unit_id LIMIT 5
$$;

CREATE OR REPLACE FUNCTION search_vectors(p_agent text,q text)
RETURNS TABLE(release_id bigint,url text,ordinal int,content text) LANGUAGE plpgsql SET search_path=hospital_refresh_lab,public AS $$
DECLARE v public.vector(1536); rel releases%ROWTYPE;
BEGIN
  SELECT s.* INTO STRICT rel FROM releases s JOIN agents a ON a.active_id=s.id WHERE a.agent=p_agent;
  IF rel.mode<>'models' THEN RAISE EXCEPTION 'Active snapshot is chunks-only; no model vectors available'; END IF;
  v:=ai.embed(text=>q, model=>rel.embedding_model, dimensions=>1536)::public.vector;
  RETURN QUERY SELECT p.release_id,p.url,p.ordinal,p.content FROM passages p
    WHERE p.release_id=rel.id ORDER BY p.embedding <=> v,p.unit_id LIMIT 5;
END $$;
COMMIT;
