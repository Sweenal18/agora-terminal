-- Agora Terminal - PostgreSQL Init
CREATE DATABASE dagster;
CREATE DATABASE agora_meta;
GRANT ALL PRIVILEGES ON DATABASE dagster TO agora;
GRANT ALL PRIVILEGES ON DATABASE agora_meta TO agora;
