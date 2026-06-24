-- Cria databases auxiliares usados por outros servicos na rede datalake.
-- Executado uma unica vez na inicializacao do container postgres_origem.

-- Banco de metadados do Metabase
SELECT 'CREATE DATABASE metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec
