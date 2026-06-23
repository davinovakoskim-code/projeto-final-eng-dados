# Imagem do Jupyter + Spark 3.5.3 usada para rodar as transformacoes (PySpark + Delta).
# Estende a imagem oficial e instala os pacotes Python do projeto (minio, dotenv,
# etc.) de forma permanente, evitando instalacao em runtime dentro do notebook.
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

# Pacotes Python necessarios para os scripts de ingestao/transformacao.
# Versoes alinhadas com o pyproject.toml do projeto.
RUN pip install --no-cache-dir \
        minio==7.2.7 \
        python-dotenv==1.0.1 \
        delta-spark==3.2.0
